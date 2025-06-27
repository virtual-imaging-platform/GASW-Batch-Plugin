package fr.insalyon.creatis.gasw.executor.batch.internals.terminal;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.util.Collection;
import java.util.EnumSet;
import java.util.concurrent.TimeUnit;

import org.apache.sshd.client.SshClient;
import org.apache.sshd.client.channel.ChannelExec;
import org.apache.sshd.client.channel.ClientChannelEvent;
import org.apache.sshd.client.session.ClientSession;
import org.apache.sshd.common.config.keys.loader.KeyPairResourceLoader;
import org.apache.sshd.common.keyprovider.KeyIdentityProvider;
import org.apache.sshd.common.util.security.SecurityUtils;
import org.apache.sshd.scp.client.ScpClient;
import org.apache.sshd.scp.client.ScpClientCreator;

import fr.insalyon.creatis.gasw.GaswException;
import fr.insalyon.creatis.gasw.executor.batch.config.json.properties.BatchConfig;
import fr.insalyon.creatis.gasw.executor.batch.config.json.properties.BatchCredentials;
import lombok.extern.log4j.Log4j;

/**
 * @see -RFC 4253
 */
@Log4j
public class RemoteTerminal {

    final private BatchConfig       config;
    final private BatchCredentials  cred;

    private SshClient       client;
    private ClientSession   session;

    public RemoteTerminal(final BatchConfig config) {
        this.config = config;
        this.cred = config.getCredentials();
    }

    /**
     * This method is called inside connect(), should not be directly called outside of test context
     */
    public void init() throws GaswException {
        try {
            final KeyPairResourceLoader loader = SecurityUtils.getKeyPairResourceParser();
            final Collection<KeyPair> keys = loader.loadKeyPairs(null, Paths.get(cred.getPrivateKeyPath()), null);

            client = SshClient.setUpDefaultClient();
            client.setKeyIdentityProvider(KeyIdentityProvider.wrapKeyPairs(keys));
            client.start();

        } catch (GeneralSecurityException | IOException e) {
            log.error("Failed to init the SshClient!");
            throw new GaswException("Failed to init the SshClient", e);
        }
    }

    public void connect() throws GaswException {
        init();

        try {
            session = client.connect(cred.getUsername(), cred.getHost(), cred.getPort())
                    .verify(config.getOptions().getSshEventTimeout(), TimeUnit.SECONDS)
                    .getClientSession();

            session.auth().verify(config.getOptions().getSshEventTimeout(), TimeUnit.SECONDS);

        } catch (IOException e) {
            log.error("Failed to connect to ssh!");
            throw new GaswException("Failed to connect to ssh", e);
        }
    }

    public void disconnect() throws GaswException {
        try {
            session.disconnect(11, "Session ended");
        } catch (IOException e) {
            log.error("Failed to disconnect from ssh!");
            throw new GaswException("Failed to disconnect", e);
        } finally {
            client.stop();
        }
    }

    public void upload(final String localFile, final String remoteLocation) throws GaswException {
        final ScpClientCreator creator = ScpClientCreator.instance();
        final ScpClient scpClient = creator.createScpClient(session);

        try {
            scpClient.upload(Paths.get(localFile), remoteLocation);
        } catch (IOException e) {
            log.error("Failed to upload file on remote!");
            throw new GaswException("Failed to upload file on remote !", e);
        }
    }

    public void download(final String remoteFile, final String localLocation) throws GaswException {
        final ScpClientCreator creator = ScpClientCreator.instance();
        final ScpClient scpClient = creator.createScpClient(session);

        try {
            scpClient.download(remoteFile, Paths.get(localLocation));
        } catch (IOException e) {
            log.error("Failed to download file on remote!");
            throw new GaswException("Failed to download file on remote !", e);
        }
    }

    private RemoteOutput executeCommand(final String command) throws GaswException {
        try (ByteArrayOutputStream stdout = new ByteArrayOutputStream();
                ByteArrayOutputStream stderr = new ByteArrayOutputStream();
                ChannelExec channel = session.createExecChannel(command)) {

            channel.setOut(stdout);
            channel.setErr(stderr);

            channel.open().verify(config.getOptions().getCommandExecutionTimeout(), TimeUnit.SECONDS);
            channel.waitFor(EnumSet.of(ClientChannelEvent.CLOSED, ClientChannelEvent.EXIT_STATUS), config.getOptions().getSshEventTimeout());

            return new RemoteOutput(stdout.toString(), stderr.toString(), channel.getExitStatus());
        } catch (IOException e) {
            log.error("Failed to execute a command on remote!", e);
            throw new GaswException("Failed to execute a commande on remote!", e);
        }
    }

    
    public RemoteOutput oneCommand(final String command) throws GaswException {
        final RemoteTerminal term = new RemoteTerminal(config);
        final RemoteOutput result;

        try {
            term.connect();
            result = term.executeCommand(command);
            term.disconnect();

            return result;
        } catch (GaswException e) {
            log.error("Failed to execute oneCommand !");
            throw new GaswException("Failed to execute oneCommand !", e);
        }
    }
}