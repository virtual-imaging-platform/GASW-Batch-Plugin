package fr.insalyon.creatis.gasw.executor.batch;

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
import lombok.extern.log4j.Log4j;

@Log4j
final public class BatchMonitor extends GaswMonitor {

    @Getter
    @Setter
    private BatchManager    manager;

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
        while (true) {
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
            }
        }
    }

    @Override
    public synchronized void add(final String jobID, final String symbolicName, final String fileName, final String parameters) throws GaswException {
        final Job job = new Job(jobID, GaswConfiguration.getInstance().getSimulationID(),
                GaswStatus.QUEUED, symbolicName, fileName, parameters,
                Constants.EXECUTOR_NAME);

        job.setQueued(new Date());
        add(job);
        log.info("Adding job: " + jobID);
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
            log.error("Error updating job status! " + job.getId(), e);
        }
    }

    @Override
    protected void kill(Job job) {
        final BatchJob batchJob = manager.getJob(job.getId());

        if (batchJob != null) {
            RemoteCommand command = batchJob.getData().getEngine().getDeleteCommand(batchJob.getData().getBatchJobID());

            try {
                command.execute(batchJob.getData().getConfig());
                log.info("Job" + job.getId() + " successfully killed!");

                updateJob(job, GaswStatus.DELETED);

            } catch (GaswException e) {
                log.warn("Failed to kill job " + job.getId());
            }
        } else {
            log.warn("Job " + job.getId() + " do not exist anymore!");
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
