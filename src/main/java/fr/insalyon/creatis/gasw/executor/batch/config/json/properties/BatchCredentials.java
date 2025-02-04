package fr.insalyon.creatis.gasw.executor.batch.config.json.properties;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class BatchCredentials {

    @JsonProperty(value = "host", required = true)
    private String host;

    @JsonProperty(value = "port", required = true)
    private int port;

    @JsonProperty(value = "username", required = true)
    private String username;

    @JsonProperty(value = "privateKeyPath", required = true)
    private String privateKeyPath;

    @JsonProperty(value = "workingDir", required = true)
    private String workingDir; /* should contain the / at the end */
}
