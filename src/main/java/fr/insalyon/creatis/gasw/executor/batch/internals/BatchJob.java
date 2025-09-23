package fr.insalyon.creatis.gasw.executor.batch.internals;

import fr.insalyon.creatis.gasw.GaswException;
import fr.insalyon.creatis.gasw.execution.GaswStatus;
import fr.insalyon.creatis.gasw.executor.batch.config.json.properties.BatchEngine;
import fr.insalyon.creatis.gasw.executor.batch.internals.commands.RemoteCommand;
import fr.insalyon.creatis.gasw.executor.batch.internals.commands.items.Cat;
import fr.insalyon.creatis.gasw.executor.batch.internals.terminal.RemoteFile;
import fr.insalyon.creatis.gasw.executor.batch.internals.terminal.RemoteTerminal;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
@Setter
public class BatchJob {

    @Getter
    final private BatchJobData data;

    @Getter
    private boolean     terminated = false;
    private GaswStatus  status = GaswStatus.NOT_SUBMITTED;

    /**
     * Upload all the data to the job directory.
     * 
     * @throws GaswException
     */
    public void prepare() throws GaswException {
        final RemoteTerminal rt = new RemoteTerminal(data.getConfig());
        new BatchFileBuilder(data).build();

        rt.connect();
        for (final RemoteFile file : data.getFilesUpload()) {
            log.info("Uploading file from {} to {}", file.getSource(), file.getDest());
            rt.upload(file.getSource(), file.getDest());
        }
        rt.disconnect();
    }

    /**
     * Download all the data created by the jobs (not the app output) but the logs.
     */
    public void download() throws GaswException {
        final RemoteTerminal rt = new RemoteTerminal(data.getConfig());

        rt.connect();
        for (final RemoteFile file : data.getFilesDownload()) {
            log.info("Downloading file from {} to {}", file.getSource(), file.getDest());
            rt.download(file.getSource(), file.getDest());
        }
        rt.disconnect();
    }

    public void submitToCluster() throws GaswException {
        final BatchEngine engine = data.getEngine();
        final RemoteCommand command = engine.getSubmitCommand(data.getRemoteBatchFile());

        try {
            command.execute(data.getConfig());

            if (command.failed()) {
                throw new GaswException("Command failed !");
            }
            data.setBatchJobID(command.result());
            log.debug("Job ID inside the Cluster: {}", command.result());

        } catch (GaswException e) {
            log.error("Failed to submit the job {}", getData().getJobID());
            throw e;
        }
    }

    public void submit() throws GaswException {
        prepare();
        submitToCluster();
        setStatus(GaswStatus.SUCCESSFULLY_SUBMITTED);
    }

    private GaswStatus convertStatus(final String status) {
        if (status == null) {
            return GaswStatus.UNDEFINED;
        }
        switch (status) {
            case "COMPLETE":
                return GaswStatus.COMPLETED;
            case "COMPLETED":
                return GaswStatus.COMPLETED;
            case "PENDING":
                return GaswStatus.QUEUED;
            case "CONFIGURING":
                return GaswStatus.QUEUED;
            case "RUNNING":
                return GaswStatus.RUNNING;
            case "FAILED":
                return GaswStatus.ERROR;
            case "NODE_FAIL":
                return GaswStatus.ERROR;
            case "BOOT_FAIL":
                return GaswStatus.ERROR;
            case "OUT_OF_MEMORY":
                return GaswStatus.ERROR;
            default:
                return GaswStatus.UNDEFINED;
        }
    }

    private GaswStatus getStatusRequest() {
        final BatchEngine engine = data.getEngine();
        final RemoteCommand command = engine.getStatusCommand(data.getBatchJobID());
        final String result;

        try {
            command.execute(data.getConfig());

            if (command.failed()) {
                return GaswStatus.UNDEFINED;
            }
            result = command.result();
            return convertStatus(result);

        } catch (GaswException e) {
            log.error("Failed to retrieve job status !", e);
            return GaswStatus.UNDEFINED;
        }
    }

    public int getExitCode() {
        final RemoteCommand command = new Cat(data.getWorkingDir() + data.getExitCodePath());

        try {
            command.execute(data.getConfig());

            if (command.failed()) {
                return 1;
            }
            return Integer.parseInt(command.result().trim());

        } catch (GaswException e) {
            log.error("Can't retrieve exitcode !", e);
            return 1;
        }
    }

    public GaswStatus getStatus() throws InterruptedException {
        GaswStatus rawStatus;

        if (status == GaswStatus.NOT_SUBMITTED || status == GaswStatus.UNDEFINED || status == GaswStatus.STALLED) {
            return status;
        }
        for (int i = 0; i < data.getConfig().getOptions().getStatusRetry(); i++) {
            rawStatus = getStatusRequest();

            if (rawStatus != GaswStatus.UNDEFINED) {
                return rawStatus;
            } else {
                Thread.sleep(data.getConfig().getOptions().getStatusRetryWait());
            }
        }
        log.warn("Max status retry reached for {}, the job status will defined as STALLED!", getData().getJobID());
        return GaswStatus.STALLED;
    }
}
