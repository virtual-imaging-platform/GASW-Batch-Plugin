package fr.insalyon.creatis.gasw.executor.batch.config.json.properties;

import java.lang.reflect.InvocationTargetException;

import fr.insalyon.creatis.gasw.executor.batch.internals.commands.RemoteCommand;
import fr.insalyon.creatis.gasw.executor.batch.internals.commands.items.Qdel;
import fr.insalyon.creatis.gasw.executor.batch.internals.commands.items.Qsub;
import fr.insalyon.creatis.gasw.executor.batch.internals.commands.items.Sbatch;
import fr.insalyon.creatis.gasw.executor.batch.internals.commands.items.Scancel;
import fr.insalyon.creatis.gasw.executor.batch.internals.commands.items.Scontrol;
import fr.insalyon.creatis.gasw.executor.batch.internals.commands.items.Tracejob;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public enum BatchEngine {

    SLURM(Sbatch.class, Scontrol.class, Scancel.class),
    PBS(Qsub.class, Tracejob.class, Qdel.class);

    final private Class<? extends RemoteCommand> submit;
    final private Class<? extends RemoteCommand> status;
    final private Class<? extends RemoteCommand> delete;

    BatchEngine(Class<? extends RemoteCommand> submitCommand, Class<? extends RemoteCommand> statusCommand,
            Class<? extends RemoteCommand> deleteCommand) {
        this.submit = submitCommand;
        this.status = statusCommand;
        this.delete = deleteCommand;
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

    public RemoteCommand getDeleteCommand(final String data) {
        return buidler(delete, data);
    }
}
