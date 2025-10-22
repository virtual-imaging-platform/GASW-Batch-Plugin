package fr.insalyon.creatis.gasw.executor.batch;

import java.io.File;

import fr.insalyon.creatis.gasw.GaswConstants;
import fr.insalyon.creatis.gasw.GaswException;
import fr.insalyon.creatis.gasw.GaswExitCode;
import fr.insalyon.creatis.gasw.GaswOutput;
import fr.insalyon.creatis.gasw.execution.GaswOutputParser;
import fr.insalyon.creatis.gasw.execution.GaswStatus;
import fr.insalyon.creatis.gasw.executor.batch.internals.BatchJob;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BatchOutputParser extends GaswOutputParser {

    final private BatchJob batchJob;

    public BatchOutputParser(final BatchJob batchJob) {
        super(batchJob.getData().getJobID());
        this.batchJob = batchJob;
    }

    @Override
    public GaswOutput getGaswOutput() throws GaswException {
        final File stdOut = getAppStdFile(GaswConstants.OUT_EXT, GaswConstants.OUT_ROOT);
        final File stdErr = getAppStdFile(GaswConstants.ERR_EXT, GaswConstants.ERR_ROOT);
        GaswExitCode gaswExitCode = GaswExitCode.UNDEFINED;
        int exitCode;

        batchJob.download();
        moveProvenanceFile(".");

        if (job.getStatus() != GaswStatus.DELETED) {
            exitCode = parseStdOut(stdOut);
            exitCode = parseStdErr(stdErr, exitCode);
            gaswExitCode = GaswExitCode.fromExitCode(exitCode);
        } else {
            gaswExitCode = GaswExitCode.EXECUTION_CANCELED;
        }

        return new GaswOutput(job.getId(), gaswExitCode, "", uploadedResults,
                appStdOut, appStdErr, stdOut, stdErr);
    }

    @Override
    protected void resubmit() throws GaswException {
        throw new GaswException("Resubmit not implemented in gasw-batch!");
    }
}
