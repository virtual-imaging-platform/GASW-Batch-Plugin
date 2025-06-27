package fr.insalyon.creatis.gasw.executor.batch.config.json.properties;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
public class BatchConfig {

    @JsonProperty(value = "credentials", required = true)
    private BatchCredentials credentials;

    @JsonProperty(value = "options", required = true)
    private BatchOptions options;
}