package fr.insalyon.creatis.gasw.executor.batch.internals.commands.items;

import fr.insalyon.creatis.gasw.executor.batch.internals.commands.RemoteCommand;

public class Scancel extends RemoteCommand {

    public Scancel(final String jobID) {
        super("scancel " + jobID);
    }

    @Override
    public String result() {
        return "";
    }
}
