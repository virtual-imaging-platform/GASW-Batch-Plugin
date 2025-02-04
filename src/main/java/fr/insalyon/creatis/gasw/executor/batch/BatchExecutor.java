package fr.insalyon.creatis.gasw.executor.batch;

import java.util.ArrayList;
import java.util.List;

import fr.insalyon.creatis.gasw.GaswConfiguration;
import fr.insalyon.creatis.gasw.GaswException;
import fr.insalyon.creatis.gasw.GaswInput;
import fr.insalyon.creatis.gasw.executor.batch.config.Constants;
import fr.insalyon.creatis.gasw.executor.batch.config.json.ConfigBuilder;
import fr.insalyon.creatis.gasw.executor.batch.config.json.properties.BatchConfig;
import fr.insalyon.creatis.gasw.executor.batch.internals.BatchManager;
import fr.insalyon.creatis.gasw.plugin.ExecutorPlugin;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j;
import net.xeoh.plugins.base.annotations.PluginImplementation;

@PluginImplementation
@NoArgsConstructor
@Log4j
public class BatchExecutor implements ExecutorPlugin {

    private BatchManager    manager;
    private BatchSubmit     submitter;
    private BatchMonitor    monitor;
    private boolean         loaded = false;

    @Override
    public String getName() {
        return Constants.EXECUTOR_NAME;
    }

    @Override
    public void load(final GaswInput gaswInput) throws GaswException {
        if ( ! loaded) {
            final BatchConfig config = new ConfigBuilder(Constants.PLUGIN_CONFIG).get();

            manager = new BatchManager(GaswConfiguration.getInstance().getSimulationID(), config);
            manager.init();

            monitor = new BatchMonitor();
            monitor.setManager(manager);
            loaded = true;
            log.info("GASW-Batch-Plugin launched with " + config.getOptions().getBatchEngine());
        }
        submitter = new BatchSubmit(gaswInput, new BatchMinorStatusGenerator(), manager, monitor);
    }

    @Override
    public List<Class> getPersistentClasses() throws GaswException {
        return new ArrayList<>();
    }

    @Override
    public String submit() throws GaswException {
        return submitter.submit();
    }

    @Override
    public void terminate() throws GaswException {
        manager.destroy();
        // Gasw
        monitor.finish();
    }
}
