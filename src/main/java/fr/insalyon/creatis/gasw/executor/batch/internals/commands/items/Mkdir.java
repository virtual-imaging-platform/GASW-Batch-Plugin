package fr.insalyon.creatis.gasw.executor.batch.internals.commands.items;

import fr.insalyon.creatis.gasw.executor.batch.internals.commands.RemoteCommand;

public class Mkdir extends RemoteCommand {

    public Mkdir(final String folderToCreate, final String options) {
        super("mkdir " + options + " " + folderToCreate);
    }

    public String result() {
        return "";
    }
}
