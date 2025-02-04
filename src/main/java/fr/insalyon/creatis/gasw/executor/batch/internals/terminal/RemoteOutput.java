package fr.insalyon.creatis.gasw.executor.batch.internals.terminal;

import lombok.Getter;

@Getter
public class RemoteOutput {

    final private RemoteStream  stdout;
    final private RemoteStream  stderr;
    final private int           exitCode;

    public RemoteOutput(final String stdout, final String stderr, final int exitCode) {
        this.stdout = new RemoteStream(stdout);
        this.stderr = new RemoteStream(stderr);
        this.exitCode = exitCode;
    }
}
