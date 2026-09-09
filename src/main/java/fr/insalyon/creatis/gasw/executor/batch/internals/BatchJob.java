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
    @Setter
    private String executionTimeSlurm;

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
    public void generateRemoteSlurmMetrics() {
        String batchJobId = data.getBatchJobID();
        String workingDir = data.getWorkingDir();
        String remoteMetricsPath = workingDir + "out/" + data.getJobID() + ".slurm.metrics";

        if (batchJobId == null || batchJobId.isEmpty()) {
            log.warn("Cannot retrieve Slurm metrics: batchJobID is null.");
            return;
        }

        try {
            String raw = fetchSacctResultWithRetry(batchJobId);
            String metricsContent;

            if (raw != null) {
                metricsContent = buildMetricsFromSacct(raw, batchJobId);
            } else {
                log.warn("No sacct data for job {} after retries, falling back to scontrol.", batchJobId);
                metricsContent = buildMetricsFromScontrolFallback(batchJobId);
            }

            if (metricsContent == null || metricsContent.isBlank()) {
                log.warn("Unable to build Slurm metrics for job {} (sacct and scontrol both failed).", batchJobId);
                return;
            }

            writeMetricsFile(remoteMetricsPath, metricsContent);

        } catch (GaswException e) {
            log.error("Failed to generate Slurm metrics for job " + batchJobId, e);
        }
    }
    private String fetchSacctResultWithRetry(String batchJobId) throws GaswException {
        final BatchEngine engine = data.getEngine();
        final int maxRetry = 3;
        final long waitMs = 3000;

        for (int i = 0; i < maxRetry; i++) {
            final RemoteCommand command = engine.getMetricsCommand(batchJobId);
            command.execute(data.getConfig());
            String raw = command.result();

            if (!command.failed() && raw != null && !raw.isBlank() && !raw.equals("|")) {
                return raw;
            }

            log.warn("sacct returned no usable data for job {} (attempt {}/{}), retrying in {} ms",
                batchJobId, i + 1, maxRetry, waitMs);

            if (i < maxRetry - 1) {
                try {
                    Thread.sleep(waitMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return null;
                }
            }
        }
        return null;
    }

    private String buildMetricsFromSacct(String raw, String batchJobId) {
        String[] fields = raw.split("\\|");
        if (fields.length < 9) {
            log.warn("Unexpected sacct output format for job {}: {}", batchJobId, raw);
            return null;
        }

        String jobid = fields[0];
        String state = fields[1];
        String exitcode = fields[2];
        String cpuUtil = fields[3];
        String memUsed = fields[4];
        String memAlloc = fields[5];
        String start = fields[6].replace("T", " ");
        String end = fields[7].replace("T", " ");
        String elapsed = normalizeElapsed(fields[8]);
        String memPct = computeMemPct(memUsed, memAlloc);

        this.setExecutionTimeSlurm(elapsed);
        log.info("Execution Time Slurm set directly to object for H2 persistence: {}", elapsed);

        return buildMetricsContent(jobid, state, exitcode, cpuUtil, memAlloc, memUsed, memPct, start, end, elapsed);
    }

    private String buildMetricsFromScontrolFallback(String batchJobId) {
        try {
            RemoteCommand fullCmd = new RemoteCommand("scontrol show job " + batchJobId + " | xargs") {
            @Override
            public String result() {
                return String.join(" ", getOutput().getStdout().getRow(0));
            }
                 };
            fullCmd.execute(data.getConfig());

            if (fullCmd.failed()) {
                log.warn("scontrol fallback also failed for job {}", batchJobId);
                return null;
            }

            String scontrolOut = fullCmd.result();

            String state = extractField(scontrolOut, "JobState");
            String exitcode = extractField(scontrolOut, "ExitCode");
            String start = extractField(scontrolOut, "StartTime").replace("T", " ");
            String end = extractField(scontrolOut, "EndTime").replace("T", " ");
            String memAlloc = extractField(scontrolOut, "MinMemoryNode");
            String elapsed = computeElapsedFromDates(start, end);

            this.setExecutionTimeSlurm(elapsed);

            return buildMetricsContent(batchJobId, state, exitcode, "N/A", memAlloc, "N/A", "N/A", start, end, elapsed);

        } catch (GaswException e) {
            log.error("scontrol fallback failed for job " + batchJobId, e);
            return null;
        }
    }

    private String extractField(String raw, String key) {
        if (raw == null) return "Unknown";
        java.util.regex.Matcher m = java.util.regex.Pattern.compile(key + "=(\\S+)").matcher(raw);
        return m.find() ? m.group(1) : "Unknown";
    }

    private String computeElapsedFromDates(String start, String end) {
        try {
            java.time.LocalDateTime s = java.time.LocalDateTime.parse(start.replace(" ", "T"));
            java.time.LocalDateTime e = java.time.LocalDateTime.parse(end.replace(" ", "T"));
            long secs = java.time.Duration.between(s, e).getSeconds();
            return String.format("%02d:%02d:%02d", secs / 3600, (secs % 3600) / 60, secs % 60);
        } catch (Exception e) {
            return "N/A";
        }
    }

    private String buildMetricsContent(String jobid, String state, String exitcode, String cpuUtil,
            String memAlloc, String memUsed, String memPct, String start, String end, String elapsed) {

        StringBuilder sb = new StringBuilder();
        sb.append("=======================================\n");
        sb.append("         SLURM JOB MONITORING           \n");
        sb.append("=======================================\n");
        sb.append("Job ID             : ").append(jobid).append("\n");
        sb.append("Job State          : ").append(state).append("\n");
        sb.append("Exit Code          : ").append(exitcode).append("\n");
        sb.append("CPU Usage %        : ").append(cpuUtil).append("\n");
        sb.append("Allocated Memory   : ").append(memAlloc).append("\n");
        sb.append("Max RAM Used       : ").append(memUsed).append("\n");
        sb.append("Memory Usage %     : ").append(memPct).append("\n");
        sb.append("Launch Time        : ").append(start).append("\n");
        sb.append("End Time           : ").append(end).append("\n");
        sb.append("Execution Time     : ").append(elapsed).append("\n");
        sb.append("======================================\n");

        return sb.toString();
    }

    private String normalizeElapsed(String elapsed) {
        if (elapsed != null && elapsed.contains("-")) {
            String[] parts = elapsed.split("-");
            int days = Integer.parseInt(parts[0]);
            String[] time = parts[1].split(":");
            int totalHours = (days * 24) + Integer.parseInt(time[0]);
            return totalHours + ":" + time[1] + ":" + time[2];
        }
        return elapsed;
    }

    private String computeMemPct(String memUsed, String memAlloc) {
        try {
            double used = extractNumeric(memUsed);
            double alloc = extractNumeric(memAlloc);
            if (alloc > 0) {
                return String.format("%.2f%%", (used / alloc) * 100);
            }
        } catch (NumberFormatException e) {
            log.debug("Could not compute memory percentage from {} / {}", memUsed, memAlloc);
        }
        return "N/A";
    }

    private double extractNumeric(String value) {
        if (value == null) {
            return 0;
        }
        String digits = value.replaceAll("[^0-9]", "");
        return digits.isEmpty() ? 0 : Double.parseDouble(digits);
    }

    private void writeMetricsFile(String remoteMetricsPath, String content) throws GaswException {
        String writeCommand = "mkdir -p " + remoteMetricsPath.substring(0, remoteMetricsPath.lastIndexOf('/'))
            + " && cat << 'EOF' > " + remoteMetricsPath + "\n"
            + content
            + "EOF";

        RemoteCommand writeCmd = new RemoteCommand(writeCommand) {
            @Override public String result() { return ""; }
        };
        writeCmd.execute(data.getConfig());

        if (writeCmd.failed()) {
            log.error("Failed to write Slurm metrics file at {}", remoteMetricsPath);
        } else {
            log.info("Slurm metrics successfully generated at: {}", remoteMetricsPath);
        }
    }
}