package fr.insalyon.creatis.gasw.executor.batch.internals.commands.items;

import fr.insalyon.creatis.gasw.executor.batch.internals.commands.RemoteCommand;

public class Qdel extends RemoteCommand {

    public Qdel(final String jobID) {
        super("qdel " + jobID);
    }

    @Override
    public String result() {
        return "";
    }
}
