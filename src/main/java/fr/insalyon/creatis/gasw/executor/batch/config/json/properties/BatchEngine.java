package fr.insalyon.creatis.gasw.executor.batch.config.json.properties;

import java.lang.reflect.InvocationTargetException;

import fr.insalyon.creatis.gasw.executor.batch.internals.commands.RemoteCommand;
import fr.insalyon.creatis.gasw.executor.batch.internals.commands.items.Qsub;
import fr.insalyon.creatis.gasw.executor.batch.internals.commands.items.Sbatch;
import fr.insalyon.creatis.gasw.executor.batch.internals.commands.items.Scontrol;
import fr.insalyon.creatis.gasw.executor.batch.internals.commands.items.Tracejob;
import lombok.extern.log4j.Log4j;

@Log4j
public enum BatchEngine {

    SLURM(Sbatch.class, Scontrol.class),
    PBS(Qsub.class, Tracejob.class);

    final private Class<? extends RemoteCommand> submit;
    final private Class<? extends RemoteCommand> status;

    BatchEngine(Class<? extends RemoteCommand> submitCommand, Class<? extends RemoteCommand> statusCommand) {
        this.submit = submitCommand;
        this.status = statusCommand;
    }

    private RemoteCommand buidler(Class<? extends RemoteCommand> toBuild, final String data) {
        try {
            return toBuild.getConstructor(String.class).newInstance(data);
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
                | NoSuchMethodException e) {
            log.error("Failed to build the command (using batch engines)", e);
        }
        return null;
    }

    public RemoteCommand getSubmitCommand(final String data) {
        return buidler(submit, data);
    }

    public RemoteCommand getStatusCommand(final String data) {
        return buidler(status, data);
    }
}
