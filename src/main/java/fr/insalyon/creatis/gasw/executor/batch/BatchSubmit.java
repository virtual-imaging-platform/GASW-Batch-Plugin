package fr.insalyon.creatis.gasw.executor.batch;

import java.util.stream.Collectors;

import fr.insalyon.creatis.gasw.GaswException;
import fr.insalyon.creatis.gasw.GaswInput;
import fr.insalyon.creatis.gasw.execution.GaswSubmit;
import fr.insalyon.creatis.gasw.executor.batch.internals.BatchManager;
import lombok.extern.log4j.Log4j;

@Log4j
public class BatchSubmit extends GaswSubmit {

    final private BatchManager manager;
    final private BatchMonitor monitor;

    public BatchSubmit(final GaswInput gaswInput, final BatchMinorStatusGenerator minorStatusServiceGenerator, 
            final BatchManager manager, final BatchMonitor monitor) throws GaswException {
        super(gaswInput, minorStatusServiceGenerator);
        scriptName = generateScript();
        this.manager = manager;
        this.monitor = monitor;
    }

    @Override
    public String submit() throws GaswException {
        final String jobID = scriptName.substring(0, scriptName.lastIndexOf("."));
        final String params = gaswInput.getParameters().stream().collect(Collectors.joining(" "));

        monitor.add(jobID, gaswInput.getExecutableName(), jobID, params);
        manager.submitJob(jobID, scriptName);

        if ( ! monitor.isAlive()) {
            monitor.start();
        }
        return jobID;
    }
}
