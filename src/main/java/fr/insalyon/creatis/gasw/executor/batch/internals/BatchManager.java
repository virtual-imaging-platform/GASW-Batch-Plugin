package fr.insalyon.creatis.gasw.executor.batch.internals;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.joda.time.DateTime;
import org.joda.time.Duration;

import fr.insalyon.creatis.gasw.GaswConfiguration;
import fr.insalyon.creatis.gasw.GaswConstants;
import fr.insalyon.creatis.gasw.GaswException;
import fr.insalyon.creatis.gasw.execution.GaswStatus;
import fr.insalyon.creatis.gasw.executor.batch.config.json.properties.BatchConfig;
import fr.insalyon.creatis.gasw.executor.batch.internals.commands.RemoteCommand;
import fr.insalyon.creatis.gasw.executor.batch.internals.commands.items.Mkdir;
import fr.insalyon.creatis.gasw.executor.batch.internals.commands.items.Rm;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j;

@Getter
@Log4j
public class BatchManager {

    final private String            workflowId;
    final private BatchConfig       config;
    final private String            workingDir;
    final private List<BatchJob>    jobs = new ArrayList<>();

    private boolean initialized = false;
    private BatchRunner runner;

    public BatchManager(final String workflowId, final BatchConfig config) {
        this.workflowId = workflowId;
        this.config = config;
        this.workingDir = config.getCredentials().getWorkingDir() + workflowId + "/";
    }

    public void init() {
        try {
            checkRemoteDirs();
            checkLocalOutputsDir();

            initialized = true;
        } catch (GaswException e) {
            log.error("Failed to init the batch manager !", e);
        }
    }

    /**
     * Create the necessary directories on the remote (batch cluster)
     */
    public void checkRemoteDirs() throws GaswException {
        final List<RemoteCommand> commands = new ArrayList<>();

        commands.add(new Mkdir(getWorkingDir(), "-p"));
        commands.add(new Mkdir(getWorkingDir() + GaswConstants.OUT_ROOT, "-p"));
        commands.add(new Mkdir(getWorkingDir() + GaswConstants.ERR_ROOT, "-p"));
        commands.add(new Mkdir(getWorkingDir() + GaswConstants.SCRIPT_ROOT, "-p"));
        commands.add(new Mkdir(getWorkingDir() + GaswConstants.INVOCATION_DIR, "-p"));
        commands.add(new Mkdir(getWorkingDir() + GaswConstants.CONFIG_DIR, "-p"));

        for (final RemoteCommand command : commands) {
            command.execute(config);

            if (command.failed()) {
                log.error("Failed to create the remotes dirs: " + command.getCommand());
                throw new GaswException("Failed to create the remotes dirs: " + command.getCommand());
            }
        }
    }

    public void checkLocalOutputsDir() {
        final String[] dirs = { GaswConstants.OUT_ROOT, GaswConstants.ERR_ROOT, "./cache" };

        Arrays.stream(dirs)
                .map(File::new)
                .forEach(dir -> {
                    if (!dir.exists()) {
                        dir.mkdirs();
                    }
                });
    }

    public void stopRunner() throws InterruptedException {
        if (runner != null) {
            runner.interrupt();
            runner.join();
        }
    }

    public void clean() {
        final RemoteCommand remoteCommand = new Rm(config.getCredentials().getWorkingDir() + workflowId, "-rf");
    
        try {
            if (remoteCommand.execute(config).failed()) {
                log.warn("Failed to execute the remote command: " + remoteCommand.getCommand());
            }
        } catch (GaswException e) {
            log.error("Failed to destroy the batch manager !", e);
        }
    }

    public void submitJob(final String jobID, final String scriptName) {
        final BatchJobData jobData = new BatchJobData(jobID, config, workflowId, scriptName);

        if (runner == null) {
            runner = new BatchRunner();
            runner.start();
        }
        synchronized (this) {
            jobs.add(new BatchJob(jobData));
        }
    }

    public BatchJob getJob(final String jobID) {
        return jobs.stream()
                .filter(job -> job.getData().getJobID().equals(jobID))
                .findFirst()
                .orElse(null);
    }

    public List<BatchJob> getUnfinishedJobs() {
        return jobs.stream()
                .filter(job -> !job.isTerminated())
                .collect(Collectors.toList());
    }

    @NoArgsConstructor
    class BatchRunner extends Thread {
        private DateTime startedTime;

        @Override
        public void run() {
            try {
                startedTime = DateTime.now();
                loop();
            } catch (GaswException e) {
                log.error("Something bad happened during the BatchRunner", e);
            } catch (InterruptedException e) {
                log.warn("InterruptedException, a soft/hard-kill may occur!");
            }
        }

        private void sleep() throws GaswException, InterruptedException {
            final Duration diff = new Duration(startedTime, DateTime.now());

            if (diff.getStandardSeconds() > config.getOptions().getTimeToBeReady()) {
                throw new GaswException("Volume wasn't eady in 2 minutes, aborting !");
            } else {
                Thread.sleep(GaswConfiguration.getInstance().getDefaultSleeptime());
            }
        }

        private void loop() throws InterruptedException, GaswException {
            while ( ! initialized) {
                sleep();
            }
            while (true) {
                synchronized (this) {
                    for (final BatchJob job : getUnfinishedJobs()) {
                        if (job.getStatus() == GaswStatus.NOT_SUBMITTED) {
                            try {
                                job.submit();
                            } catch (GaswException e) {
                                log.error("Failed to submit the job !", e);
                                job.setStatus(GaswStatus.ERROR);
                            }
                        }
                    }
                }
                Thread.sleep(GaswConfiguration.getInstance().getDefaultSleeptime());
            }
        }
    }
}
