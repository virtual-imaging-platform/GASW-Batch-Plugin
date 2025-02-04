package fr.insalyon.creatis.gasw.executor.batch.internals.commands;

import fr.insalyon.creatis.gasw.GaswException;
import fr.insalyon.creatis.gasw.executor.batch.config.json.properties.BatchConfig;
import fr.insalyon.creatis.gasw.executor.batch.internals.terminal.RemoteOutput;
import fr.insalyon.creatis.gasw.executor.batch.internals.terminal.RemoteTerminal;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public abstract class RemoteCommand {

    @Getter
    final private String command;

    @Getter
    private RemoteOutput output;

    public RemoteCommand execute(final BatchConfig config) throws GaswException {
        output = new RemoteTerminal(config).oneCommand(command);
        return this;
    }

    public boolean failed() {
        if (output != null) {
            return (output == null || output.getExitCode() != 0);
        } else {
            return true;
        }
    }

    public abstract String result();
}
