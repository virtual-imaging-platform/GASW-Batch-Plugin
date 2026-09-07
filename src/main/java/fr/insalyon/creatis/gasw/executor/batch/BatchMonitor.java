package fr.insalyon.creatis.gasw.executor.batch;

import java.util.Arrays;
import java.util.Date;

import fr.insalyon.creatis.gasw.GaswConfiguration;
import fr.insalyon.creatis.gasw.GaswException;
import fr.insalyon.creatis.gasw.bean.Job;
import fr.insalyon.creatis.gasw.dao.DAOException;
import fr.insalyon.creatis.gasw.execution.GaswMonitor;
import fr.insalyon.creatis.gasw.execution.GaswStatus;
import fr.insalyon.creatis.gasw.executor.batch.config.Constants;
import fr.insalyon.creatis.gasw.executor.batch.internals.BatchJob;
import fr.insalyon.creatis.gasw.executor.batch.internals.BatchManager;
import fr.insalyon.creatis.gasw.executor.batch.internals.commands.RemoteCommand;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final public class BatchMonitor extends GaswMonitor {

    @Getter
    @Setter
    private BatchManager    manager;
    @Setter
    private boolean         stop = false;

    public BatchMonitor() {
        super();
    }

    private boolean notRunningJob(GaswStatus s) {
        return s != GaswStatus.RUNNING
            && s != GaswStatus.QUEUED
            && s != GaswStatus.UNDEFINED
            && s != GaswStatus.NOT_SUBMITTED;
    }

    @Override
    public void run() {
        while ( ! stop) {
            verifySignaledJobs();
            try {
                for (final BatchJob job : manager.getUnfinishedJobs()) {
                    final Job daoJob = jobDAO.getJobByID(job.getData().getJobID());
                    final GaswStatus status = job.getStatus();

                    if (notRunningJob(status)) {
                        job.setTerminated(true);
                        if (status == GaswStatus.ERROR || status == GaswStatus.COMPLETED) {
                            daoJob.setExitCode(job.getExitCode());
                            daoJob.setStatus(job.getExitCode() == 0 ? GaswStatus.COMPLETED : GaswStatus.ERROR);
                        } else {
                            daoJob.setStatus(status);
                        }

                        jobDAO.update(daoJob);
                        if (status == GaswStatus.COMPLETED || status == GaswStatus.ERROR) {
                            generateRemoteSlurmMetrics(job);
                        }
                        new BatchOutputParser(job).start();

                    } else if (status == GaswStatus.RUNNING) {
                        updateJob(daoJob, status);
                    }
                }
                Thread.sleep(GaswConfiguration.getInstance().getDefaultSleeptime());

            } catch (GaswException | DAOException ex) {
                log.error("Exception while monitoring batch jobs. Ignoring to continue the monitoring!", ex);
            } catch (InterruptedException ex) {
                log.error("Interrupted exception, stopping the worker!");
                finish();
                break;
            }
        }
        }

    private void generateRemoteSlurmMetrics(final BatchJob job) {
    String batchJobId = job.getData().getBatchJobID();
    String workingDir = job.getData().getWorkingDir();
    String remoteMetricsPath = workingDir + "out/" + job.getData().getJobID() + ".slurm.metrics";
    String scriptPath = workingDir + "out/get_metrics_" + batchJobId + ".sh";

    if (batchJobId == null || batchJobId.isEmpty()) {
        log.warn("Cannot retrieve Slurm metrics: batchJobID is null.");
        return;
    }
    try {
        log.info("Creating temporary Slurm metric script for batchJobID: {}", batchJobId);
        String createScriptCommand = "mkdir -p " + workingDir + " && cat << 'EOF' > " + scriptPath + "\n"
            + "#!/bin/bash\n"
            + "# Try to get metrics from database first\n"
            + "metrics=$(sacct -j " + batchJobId + " --format=JobID,State,ExitCode,CPUUtil,MaxRSS,ReqMem,Start,End,Elapsed --noheader --parsable2 2>/dev/null | grep -E 'batch' | head -n 1)\n"
            + "[ -z \"$metrics\" ] && metrics=$(sacct -j " + batchJobId + " --format=JobID,State,ExitCode,CPUUtil,MaxRSS,ReqMem,Start,End,Elapsed --noheader --parsable2 2>/dev/null | head -n 1)\n"
            + "\n"
            + "if [ ! -z \"$metrics\" ] && [ \"$metrics\" != \"|\" ]; then\n"
            + "    # Data found in accounting database (Production)\n"
            + "    IFS='|' read -r jobid state exitcode cpu_util mem_used mem_alloc start end elapsed <<< \"$metrics\"\n"
            + "    \n"
            + "    if [[ \"$elapsed\" == *\"-\"* ]]; then\n"
            + "        days=$(echo $elapsed | cut -d'-' -f1)\n"
            + "        time=$(echo $elapsed | cut -d'-' -f2)\n"
            + "        total_hours=$(( (days * 24) + $(echo $time | cut -d':' -f1) ))\n"
            + "        elapsed=\"$total_hours:$(echo $time | cut -d':' -f2):$(echo $time | cut -d':' -f3)\"\n"
            + "    fi\n"
            + "    num_used=$(echo \"$mem_used\" | grep -oE '[0-9]+')\n"
            + "    num_alloc=$(echo \"$mem_alloc\" | grep -oE '[0-9]+')\n"
            + "    if [ ! -z \"$num_used\" ] && [ ! -z \"$num_alloc\" ] && [ \"$num_alloc\" -gt 0 ]; then\n"
            + "        mem_pct=$(awk -v u=\"$num_used\" -v a=\"$num_alloc\" 'BEGIN { printf \"%.2f%%\", (u/a)*100 }')\n"
            + "    else\n"
            + "        mem_pct=\"N/A\"\n"
            + "    fi\n"
            + "else\n"
            + "    # Fallback to scontrol for live/local environments (VM)\n"
            + "    scontrol_out=$(scontrol show job " + batchJobId + " 2>/dev/null)\n"
            + "    jobid=\"" + batchJobId + "\"\n"
            + "    state=$(echo \"$scontrol_out\" | grep -oP 'JobState=\\K\\S+')\n"
            + "    exitcode=$(echo \"$scontrol_out\" | grep -oP 'ExitCode=\\K\\S+')\n"
            + "    cpu_util=\"N/A\"\n"
            + "    mem_used=\"N/A\"\n"
            + "    mem_alloc=$(echo \"$scontrol_out\" | grep -oP 'MinMemoryNode=\\K\\S+')\n"
            + "    mem_pct=\"N/A\"\n"
            + "    start=$(echo \"$scontrol_out\" | grep -oP 'StartTime=\\K\\S+')\n"
            + "    end=$(echo \"$scontrol_out\" | grep -oP 'EndTime=\\K\\S+')\n"
            + "    elapsed=\"N/A\"\n"
            + "    # Calculate duration in HH:MM:SS if start/end are valid\n"
            + "    if [ ! -z \"$start\" ] && [ ! -z \"$end\" ] && [ \"$start\" != \"Unknown\" ] && [ \"$end\" != \"Unknown\" ]; then\n"
            + "        diff_sec=$(($(date -d \"$end\" +%s) - $(date -d \"$start\" +%s)))\n"
            + "        elapsed=$(printf '%02d:%02d:%02d' $((diff_sec/3600)) $((diff_sec%3600/60)) $((diff_sec%60)))\n"
            + "    fi\n"
            + "fi\n"
            + "\n"
            + "# Cleanup date format: replace 'T' with space\n"
            + "start=$(echo \"$start\" | sed 's/T/ /')\n"
            + "end=$(echo \"$end\" | sed 's/T/ /')\n"
            + "\n"
            + "echo \"=======================================\" > " + remoteMetricsPath + "\n"
            + "echo \"         SLURM JOB MONITORING           \" >> " + remoteMetricsPath + "\n"
            + "echo \"=======================================\" >> " + remoteMetricsPath + "\n"
            + "echo \"Job ID             : $jobid\" >> " + remoteMetricsPath + "\n"
            + "echo \"Job State          : $state\" >> " + remoteMetricsPath + "\n"
            + "echo \"Exit Code          : $exitcode\" >> " + remoteMetricsPath + "\n"
            + "echo \"CPU Usage %        : $cpu_util\" >> " + remoteMetricsPath + "\n"
            + "echo \"Allocated Memory   : $mem_alloc\" >> " + remoteMetricsPath + "\n"
            + "echo \"Max RAM Used       : $mem_used\" >> " + remoteMetricsPath + "\n"
            + "echo \"Memory Usage %     : $mem_pct\" >> " + remoteMetricsPath + "\n"
            + "echo \"Launch Time        : $start\" >> " + remoteMetricsPath + "\n"
            + "echo \"End Time           : $end\" >> " + remoteMetricsPath + "\n"
            + "echo \"Execution Time     : $elapsed\" >> " + remoteMetricsPath + "\n"
            + "echo \"======================================\" >> " + remoteMetricsPath + "\n"
            + "EOF";

        RemoteCommand createCmd = new RemoteCommand(createScriptCommand) {
            @Override public String result() { return ""; }
        };
        createCmd.execute(job.getData().getConfig());

        String runAndCleanCommand = "chmod +x " + scriptPath + " && " + scriptPath + " && rm -f " + scriptPath;
        
        RemoteCommand runCmd = new RemoteCommand(runAndCleanCommand) {
            @Override public String result() { return ""; }
        };
        
        log.info("Executing Slurm metric script on frontend...");
        runCmd.execute(job.getData().getConfig());

        if (runCmd.failed()) {
            log.error("Slurm metric script execution failed for job {}", batchJobId);
        } else {
            log.info("Slurm metrics successfully generated at: {}", remoteMetricsPath);
        }


        // EXTRACT THE SLURMEXECUTIO TIME FROM THE FILE
        // String getSlurmTimeCmd = "scontrol show job " + batchJobId + " | grep -oP 'RunTime=\\K\\S+'";
        
        // RemoteCommand timeCmd = new RemoteCommand(getSlurmTimeCmd) {
        //     private String output = "";
        //     @Override
        //     public String result() { return output; }
        // };
        
        // timeCmd.execute(job.getData().getConfig());
        // String slurmTime = timeCmd.result().trim();
        
        // job.setExecutionTimeSlurm(slurmTime);
        // log.info("Execution Time Slurm extrait directement : {}", slurmTime);
        
       setSlurmExecutionTimeDirect(job);

    } catch (GaswException e) {
        log.error("Failed to generate Slurm metrics via RemoteCommand for job " + batchJobId, e);
    }
}

    private void setSlurmExecutionTimeDirect(final BatchJob job) {
        String batchJobId = job.getData().getBatchJobID();
        if (batchJobId == null || batchJobId.isEmpty()) {
            log.warn("Cannot retrieve Slurm execution time: batchJobID is null");
            return;
        }

        try {
            log.info("Retrieving Slurm execution time directly via command for batchJobID: {}", batchJobId);

            String cmd = "elapsed=$(sacct -j " + batchJobId + " --format=Elapsed --noheader 2>/dev/null | grep -v '^$' | head -n 1 | xargs); "
                    + "[ -z \"$elapsed\" ] && elapsed=$(scontrol show job " + batchJobId + " 2>/dev/null | grep -oP 'RunTime=\\K\\S+'); "
                    + "echo \"$elapsed\"";

            RemoteCommand timeCmd = new RemoteCommand(cmd) {
                private String output = "";

                @Override
                public String result() {
                    return output;
                }
            };

            timeCmd.execute(job.getData().getConfig());

            String slurmTime = timeCmd.result();

            if (slurmTime != null && !slurmTime.isBlank()) {
                slurmTime = slurmTime.trim();
                
                job.setExecutionTimeSlurm(slurmTime);
                log.info("Execution Time Slurm set directly to object for H2 persistence: {}", slurmTime);
            } else {
                log.warn("Slurm execution time command returned empty result for batchJobID: {}", batchJobId);
            }

        } catch (GaswException e) {
            log.error("Failed to retrieve Slurm execution time for job " + batchJobId, e);
        }
    }

    @Override
    public synchronized void add(final String jobID, final String symbolicName, final String fileName, final String parameters) throws GaswException {
        final Job job = new Job(jobID, GaswConfiguration.getInstance().getSimulationID(),
                GaswStatus.QUEUED, symbolicName, fileName, parameters,
                Constants.EXECUTOR_NAME,null);

        job.setQueued(new Date());
        add(job);
        log.info("Adding job: {}", jobID);
    }

    public synchronized void stopMonitor(boolean force) throws InterruptedException {
        if (force) {
            interrupt();
        } else {
            setStop(true);
        }
        join();
    }

    public synchronized void finish() {
        log.info("Monitor is off !");

        // kill jobs that are still running (context of soft-kill)
        try {
            for (Job job : jobDAO.getActiveJobs()) {
                kill(job);
            }
        } catch (DAOException e) {
            log.warn("Failed to kill the running jobs before terminating!", e);
        }
    }

    public void updateJob(final Job job, final GaswStatus status) {
        try {
            if (job.getStatus() != status) {
                if (status == GaswStatus.RUNNING) {
                    job.setDownload(new Date());
                }

                job.setStatus(status);
                jobDAO.update(job);
            }
        } catch (DAOException e) {
            log.error("Error updating job {} status!", job.getId(), e);
        }
    }

    @Override
    protected void kill(Job job) {
        final BatchJob batchJob = manager.getJob(job.getId());

        if (batchJob != null) {
            RemoteCommand command = batchJob.getData().getEngine().getDeleteCommand(batchJob.getData().getBatchJobID());

            try {
                command.execute(batchJob.getData().getConfig());
                log.info("Job {} successfully killed!", job.getId());

                updateJob(job, GaswStatus.DELETED);
                new BatchOutputParser(batchJob).start();

            } catch (GaswException e) {
                log.warn("Failed to kill job {}", job.getId());
            }
        } else {
            log.warn("Job {} do not exist anymore!", job.getId());
        }
    }

    @Override
    protected void reschedule(Job job) {}

    @Override
    protected void replicate(Job job) {}

    @Override
    protected void killReplicas(Job job) {}

    @Override
    protected void resume(Job job) {}
}