package fr.insalyon.creatis.gasw.executor.batch.internals;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class BatchFile {

    final private BatchJobData  data;
    final private StringBuilder builder = new StringBuilder(1024);

    public String build() {
        switch (data.getConfig().getOptions().getBatchEngine()) {
            case PBS:
                doPBS();
                break;
            case SLURM:
                doSlurm();
                break;
        }
        doCommon();

        return builder.toString();
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
