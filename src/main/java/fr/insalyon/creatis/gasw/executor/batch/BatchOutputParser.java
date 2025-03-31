package fr.insalyon.creatis.gasw.executor.batch;

import java.io.File;

import fr.insalyon.creatis.gasw.GaswConstants;
import fr.insalyon.creatis.gasw.GaswException;
import fr.insalyon.creatis.gasw.GaswExitCode;
import fr.insalyon.creatis.gasw.GaswOutput;
import fr.insalyon.creatis.gasw.execution.GaswOutputParser;
import fr.insalyon.creatis.gasw.execution.GaswStatus;
import fr.insalyon.creatis.gasw.executor.batch.internals.BatchJob;
import lombok.extern.log4j.Log4j;

@Log4j
public class BatchOutputParser extends GaswOutputParser {

    final private BatchJob job;

    public BatchOutputParser(final BatchJob job) {
        super(job.getData().getJobID());
        this.job = job;
    }

    @Override
    public GaswOutput getGaswOutput() throws GaswException {
        final File stdOut = getAppStdFile(GaswConstants.OUT_EXT, GaswConstants.OUT_ROOT);
        final File stdErr = getAppStdFile(GaswConstants.ERR_EXT, GaswConstants.ERR_ROOT);
        GaswExitCode gaswExitCode = GaswExitCode.UNDEFINED;
        int exitCode;

        job.download();
        moveProvenanceFile(".");

        try {
            if (job.getStatus() != GaswStatus.CANCELLED && job.getStatus() != GaswStatus.DELETED) {
                exitCode = parseStdOut(stdOut);
                exitCode = parseStdErr(stdErr, exitCode);
        
                switch (exitCode) {
                    case 0:
                        gaswExitCode = GaswExitCode.SUCCESS;
                        break;
                    case 1:
                        gaswExitCode = GaswExitCode.ERROR_READ_GRID;
                        break;
                    case 2:
                        gaswExitCode = GaswExitCode.ERROR_WRITE_GRID;
                        break;
                    case 6:
                        gaswExitCode = GaswExitCode.EXECUTION_FAILED;
                        break;
                    case 7:
                        gaswExitCode = GaswExitCode.ERROR_WRITE_LOCAL;
                        break;
                    default:
                        gaswExitCode = GaswExitCode.UNDEFINED;
                }
            } else {
                gaswExitCode = GaswExitCode.EXECUTION_CANCELED;
            }
        } catch (InterruptedException e) {
            log.warn("Failed to retrieve gaswStatus during output parsing!");
        }

        return new GaswOutput(job.getData().getJobID(), gaswExitCode, "", uploadedResults,
                appStdOut, appStdErr, stdOut, stdErr);
    }

    @Override
    protected void resubmit() throws GaswException {
        throw new GaswException("Resubmit not implemented in gasw-batch!");
    }
}
