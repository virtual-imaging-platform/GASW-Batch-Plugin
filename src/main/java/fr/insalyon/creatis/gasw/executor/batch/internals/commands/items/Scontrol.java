package fr.insalyon.creatis.gasw.executor.batch.internals.commands.items;

import fr.insalyon.creatis.gasw.executor.batch.internals.commands.RemoteCommand;

public class Scontrol extends RemoteCommand {

    public Scontrol(final String jobID) {
        super("scontrol show job " + jobID + " | grep \"JobState\" | xargs");
    }

    public String result() {
        final String[] line = getOutput().getStdout().getRow(0);

        return line[0].split("=")[1];
    }
}
