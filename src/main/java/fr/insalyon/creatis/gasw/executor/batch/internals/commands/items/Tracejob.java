package fr.insalyon.creatis.gasw.executor.batch.internals.commands.items;

import fr.insalyon.creatis.gasw.GaswException;
import fr.insalyon.creatis.gasw.executor.batch.config.json.properties.BatchConfig;
import fr.insalyon.creatis.gasw.executor.batch.internals.commands.RemoteCommand;
import fr.insalyon.creatis.gasw.executor.batch.internals.terminal.RemoteStream;

public class Tracejob extends RemoteCommand {

    final private Qstat subCommand;

    /**
     * Relies on a first call of qstat class because tracing is easier with qstat
     * but it will only work if it still running. That's why the fallback is handled by 
     * a call to the tracejob command.
     */
    public Tracejob(final String jobID) {
        super("2>/dev/null tracejob " + jobID + " | grep state | tail -n 1 | xargs");
        subCommand = new Qstat(jobID);
    }

    @Override
    public RemoteCommand execute(BatchConfig config) throws GaswException {
        subCommand.execute(config);
        return super.execute(config);
    }

    public String result() {
        if (subCommand.result().isEmpty()) {
            return traceJobResult();
        } else {
            return subCommand.result();
        }
    }

    public String traceJobResult() {
        final RemoteStream out = getOutput().getStdout();
        final String[] line = out.getRow(out.getLines().length - 1);

        return line[line.length - 1];
    }
}