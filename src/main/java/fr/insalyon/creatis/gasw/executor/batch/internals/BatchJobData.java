package fr.insalyon.creatis.gasw.executor.batch.internals;

import java.util.List;

import fr.insalyon.creatis.gasw.GaswConstants;
import fr.insalyon.creatis.gasw.executor.batch.config.json.properties.BatchConfig;
import fr.insalyon.creatis.gasw.executor.batch.config.json.properties.BatchEngine;
import fr.insalyon.creatis.gasw.executor.batch.internals.terminal.RemoteFile;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class BatchJobData {

    final private String        jobID;
    final private BatchConfig   config;

    // this is the id given by the batch system
    private String              batchJobID = null;
    private String              scriptName;
    private String              workingDir;
    private List<RemoteFile>    filesUpload;
    private List<RemoteFile>    filesDownload;

    public BatchJobData(final String jobID, final BatchConfig config, final String workflowID, final String scriptName) {
        this.workingDir = config.getCredentials().getWorkingDir() + workflowID + "/";
        this.jobID = jobID;
        this.config = config;
        this.scriptName = scriptName;

        defineDownloads();
        defineUploads();
    }

    private void defineUploads() {
        filesUpload.add(new RemoteFile(
            GaswConstants.INVOCATION_DIR + "/" + jobID + "-invocation.json", 
            workingDir));

        filesUpload.add(new RemoteFile(
            GaswConstants.CONFIG_DIR + "/" + jobID + "-configuration.sh", 
            workingDir));

        filesUpload.add(new RemoteFile(
            GaswConstants.SCRIPT_ROOT + "/" + jobID + ".sh", 
            workingDir + "/sh"));

        filesUpload.add(new RemoteFile(
            "./workflow.json", 
            workingDir));

        filesUpload.add(new RemoteFile(
            getLocalBatchFile(), 
            getRemoteBatchFile()));
    }

    private void defineDownloads() {
        filesDownload.add(new RemoteFile(
            getStderrPath(), 
            GaswConstants.ERR_ROOT + "/" + jobID + ".sh.err"));

        filesDownload.add(new RemoteFile(
            getStdoutPath(), 
            GaswConstants.OUT_ROOT + "/" + jobID + ".sh.out"));

        filesDownload.add(new RemoteFile(
            workingDir + jobID + ".sh.provenance.json", 
            "./" + jobID + ".sh.provenance.json"));
    }

    public String getExitCodePath() {
        return getJobID() + ".exit";
    }

    public String getStdoutPath() {
        return getWorkingDir() + GaswConstants.OUT_ROOT + "/" + getJobID() + GaswConstants.OUT_EXT;
    }

    public String getStderrPath() {
        return getWorkingDir() + GaswConstants.ERR_ROOT + "/" + getJobID() + GaswConstants.ERR_EXT;
    }

    public String getRemoteBatchFile() {
        return getWorkingDir() + getJobID() + ".batch";
    }

    public String getLocalBatchFile() {
        return "./" + jobID + ".batch";
    }

    public String getCommand() {
        return "bash " + GaswConstants.SCRIPT_ROOT + "/" + scriptName;
    }

    public BatchEngine getEngine() {
        return config.getOptions().getBatchEngine();
    }
}
