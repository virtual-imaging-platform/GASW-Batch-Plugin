package fr.insalyon.creatis.gasw.executor.batch.internals.commands.items;

import fr.insalyon.creatis.gasw.executor.batch.internals.commands.RemoteCommand;
import fr.insalyon.creatis.gasw.executor.batch.internals.terminal.RemoteStream;

public class Qstat extends RemoteCommand {

    public Qstat(final String jobID) {
        super("qstat -f " + jobID + " | grep job_state | xargs");
    }

    @Override
    public String result() {
        if (failed() || getOutput().getStdout().getContent().isBlank()) {
            return "";
        } else {
            final RemoteStream out = getOutput().getStdout();

            switch (out.getColumn(2)[0]) {
                case "C":
                    return "COMPLETED";
                case "E":
                    return "RUNNING";
                case "H": // held
                    return "PENDING";
                case "Q":
                    return "PENDING";
                case "R":
                    return "RUNNING";
                case "T": // moved
                    return "PENDING";
                case "W":
                    return "PENDING";
                default:
                    return "";
            }
        }
    }
}
