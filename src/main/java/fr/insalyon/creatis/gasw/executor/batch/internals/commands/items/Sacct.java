package fr.insalyon.creatis.gasw.executor.batch.internals.commands.items;

import fr.insalyon.creatis.gasw.executor.batch.internals.commands.RemoteCommand;

public class Sacct extends RemoteCommand {

    private static final String FORMAT =
        "JobID,State,ExitCode,CPUUtil,MaxRSS,ReqMem,Start,End,Elapsed";

    public Sacct(String batchJobId) {
        super("sacct -j " + batchJobId
            + " --format=" + FORMAT
            + " --noheader --parsable2 2>/dev/null | grep -E 'batch' | head -n 1");
    }

    public String result() {
            final String[] line = getOutput().getStdout().getRow(0);

            return line[0];
        }
}