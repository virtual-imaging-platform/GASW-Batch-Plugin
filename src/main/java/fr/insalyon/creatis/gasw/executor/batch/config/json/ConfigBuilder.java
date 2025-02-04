package fr.insalyon.creatis.gasw.executor.batch.config.json;

import java.io.File;
import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;

import fr.insalyon.creatis.gasw.executor.batch.config.json.properties.BatchConfig;
import lombok.extern.log4j.Log4j;

@Log4j
public class ConfigBuilder {

    final private String filePath;

    public ConfigBuilder(final String filePath) {
        this.filePath = filePath;
    }

    public BatchConfig get() {
        final ObjectMapper mapper = new ObjectMapper();

        try {
            return mapper.readValue(new File(filePath), BatchConfig.class);

        } catch (IOException e) {
            log.error("Failed to read the configuration file", e);
            return null;
        }
    }
}
