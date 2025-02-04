package fr.insalyon.creatis.gasw.executor.batch.config.json.properties;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
public class BatchOptions {

    /* value in millis */
    @JsonProperty(value = "commandExecutionTimeoutInMillis")
    private int commandExecutionTimeout = 10;

    /* value in millis */
    @JsonProperty(value = "sshEventTimeoutInMillis")
    private int sshEventTimeout = 1000;

    @JsonProperty(value = "statusRetry")
    private int statusRetry = 10;

    /* value in millis */
    @JsonProperty(value = "statusRetryWaitInMillis")
    private int statusRetryWait = 10;

    /* By default the system uses slurm */
    @JsonProperty(value = "batchEngine")
    private BatchEngine batchEngine = BatchEngine.SLURM;

    @JsonProperty("timeToBeReadyInSeconds")
    private int timeToBeReady = 120;
}
