package fr.insalyon.creatis.gasw.executor.batch.internals;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import fr.insalyon.creatis.gasw.GaswException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class BatchFileBuilder {

    final private BatchJobData  data;
    final private StringBuilder builder = new StringBuilder(1024);

    public void build() throws GaswException {
        switch (data.getConfig().getOptions().getBatchEngine()) {
            case PBS:
                doPBS();
                break;
            case SLURM:
                doSlurm();
                break;
        }
        doCustoms();
        doCommon();
        createFile();
    }

    private void createFile() throws GaswException {
        File batchFile = new File(data.getLocalBatchFile());

        try {
            if ( ! batchFile.exists()) {
                batchFile.createNewFile();
            }
            try (FileOutputStream stream = new FileOutputStream(batchFile)) {
                stream.write(builder.toString().getBytes());
            }
        } catch (IOException e) {
            log.error("Failed to create the batch file!");
            throw new GaswException("Failed to create the batch file!", e);
        }
    }

    private void doCustoms() {
        List<String> customCmds = data.getConfig().getOptions().getCustomPreJobCommands();

        builder.append("## Custom commands from config ##\n");
        for (String cmd : customCmds) {
            builder.append(cmd + "\n");
        }
        builder.append("## ##\n");
    }

    private void doCommon() {
        builder.append("cd " + data.getWorkingDir() + "\n")
                .append(data.getCommand() + "\n")
                .append("echo $? > " + data.getExitCodePath() + "\n");
    }

    private void doSlurm() {
        builder.append("#!/bin/sh\n")
                .append("#SBATCH --job-name=" + data.getJobID() + "\n")
                .append("#SBATCH --output=" + data.getStdoutPath() + "\n")
                .append("#SBATCH --error=" + data.getStderrPath() + "\n");
    }

    private void doPBS() {
        builder.append("#!/bin/sh\n")
                .append("#PBS -N " + data.getJobID() + "\n")
                .append("#PBS -o " + data.getStdoutPath() + "\n")
                .append("#PBS -e " + data.getStderrPath() + "\n");
    }
}
