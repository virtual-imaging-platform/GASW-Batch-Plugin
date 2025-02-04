package fr.insalyon.creatis.gasw.executor.batch;

import fr.insalyon.creatis.gasw.execution.GaswMinorStatusServiceGenerator;
import lombok.NoArgsConstructor;

/**
 * @deprecated
 */
@NoArgsConstructor
public class BatchMinorStatusGenerator extends GaswMinorStatusServiceGenerator {

    @Override
    public String getClient() {
        return null;
    }

    @Override
    public String getServiceCall() {
        return null;
    }
}
