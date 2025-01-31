package fr.insalyon.creatis.gasw.executor.batch.internals;

import java.util.List;

import fr.insalyon.creatis.gasw.executor.batch.config.json.properties.BatchConfig;
import fr.insalyon.creatis.gasw.executor.batch.config.json.properties.BatchEngines;
import fr.insalyon.creatis.gasw.executor.batch.internals.terminal.RemoteFile;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@RequiredArgsConstructor
public class BatchJobData {

    final private String        jobID;
    final private BatchConfig   config;

    // this is the id given by the batch system
    private String              batchJobID = null;
    private String              command;
    private String              workingDir;
    private List<RemoteFile>    filesUpload;
    private List<RemoteFile>    filesDownload;

    public String getExitCodePath() {
        return getJobID() + ".exit";
    }

    public String getStdoutPath() {
        return getWorkingDir() + "out/" + getJobID() + ".out";
    }

    public String getStderrPath() {
        return getWorkingDir() + "err/" + getJobID() + ".err";
    }

    public String getBatchFile() {
        return getWorkingDir() + getJobID() + ".batch";
    }

    public BatchEngines getEngine() {
        return config.getOptions().getBatchEngine();
    }
}
