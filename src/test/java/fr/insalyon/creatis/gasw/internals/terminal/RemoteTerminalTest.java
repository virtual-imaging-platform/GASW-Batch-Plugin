package fr.insalyon.creatis.gasw.internals.terminal;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import fr.insalyon.creatis.gasw.executor.batch.config.json.properties.BatchConfig;
import fr.insalyon.creatis.gasw.executor.batch.config.json.properties.BatchCredentials;
import fr.insalyon.creatis.gasw.executor.batch.internals.terminal.RemoteTerminal;

public class RemoteTerminalTest {

    @Mock
    private BatchConfig config;

    @Mock
    private BatchCredentials cred;

    @BeforeEach
    public void defineConfig() {
        MockitoAnnotations.openMocks(this);

        when(config.getCredentials()).thenReturn(cred);
    }

    @Test
    public void rsaKeyLoading() {
        when(cred.getPrivateKeyPath()).thenReturn("src/test/ressources/keys/rsa/dumb_key");

        RemoteTerminal terminalTest = new RemoteTerminal(config);
        assertDoesNotThrow(() -> terminalTest.init());
    }

    @Test
    public void ed255119KeyLoading() {
        when(cred.getPrivateKeyPath()).thenReturn("src/test/ressources/keys/ed255119/dumb_key");

        RemoteTerminal terminalTest = new RemoteTerminal(config);
        assertDoesNotThrow(() -> terminalTest.init());
    }
}
