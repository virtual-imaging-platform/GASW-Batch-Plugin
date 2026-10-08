package fr.insalyon.creatis.gasw.executor.batch.internals.commands;

import fr.insalyon.creatis.gasw.GaswException;
import fr.insalyon.creatis.gasw.executor.batch.config.json.properties.BatchConfig;
import fr.insalyon.creatis.gasw.executor.batch.internals.terminal.RemoteOutput;
import fr.insalyon.creatis.gasw.executor.batch.internals.terminal.RemoteTerminal;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
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
        return output == null || output.getExitCode() == null || output.getExitCode() != 0;
    }

    public void logFailure() {
        if (output == null) {
            log.error("Command {} failed : RemoteOutput null", command);
            return;
        }
        if (output.getExitCode() != null && output.getExitCode() == 0) {
            // OK, nothing to log
            return;
        }
        if (output.getExitCode() == null) {
            log.error("Command {} failed : RemoteOutput::getExitCode null", command);
        } else {
            log.error("Command {} failed : RemoteOutput::getExitCode {}}", command, output.getExitCode());
        }
        if (output.getStdout() == null) {
            log.error("RemoteOutput::getStdout null");
        } else {
            log.error("Stdout : {}", output.getStdout().getContent());
        }
        if (output.getStderr() == null) {
            log.error("RemoteOutput::getStderr null");
        } else {
            log.error("Stderr : {}", output.getStderr().getContent());
        }
    }

    public abstract String result();
}
