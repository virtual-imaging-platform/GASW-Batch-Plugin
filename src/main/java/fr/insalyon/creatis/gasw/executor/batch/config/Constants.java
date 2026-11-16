package fr.insalyon.creatis.gasw.executor.batch.config;

import fr.insalyon.creatis.gasw.GaswConfiguration;
import fr.insalyon.creatis.gasw.GaswException;

public class Constants {

    public static String getPluginConfig() throws GaswException {
        return GaswConfiguration.getInstance().getConfigDir() + "/executor.json";
    }

    public final static String SPLIT_REGEX = "\\s+";

    // GASW
    public final static String EXECUTOR_NAME = "Batch";
}
