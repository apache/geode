/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.net;


import java.io.Console;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SNIServerName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLProtocolException;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.StandardConstants;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.GemFireConfigException;
import org.apache.geode.SystemFailure;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.cache.wan.GatewayTransportFilter;
import org.apache.geode.distributed.ClientSocketFactory;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.distributed.internal.tcpserver.AdvancedSocketCreatorImpl;
import org.apache.geode.distributed.internal.tcpserver.HostAndPort;
import org.apache.geode.distributed.internal.tcpserver.TcpSocketCreatorImpl;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.admin.SSLConfig;
import org.apache.geode.internal.cache.wan.TransportFilterServerSocket;
import org.apache.geode.internal.cache.wan.TransportFilterSocketFactory;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.internal.util.ArgumentRedactor;
import org.apache.geode.internal.util.PasswordUtil;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.internal.SSLUtil;
import org.apache.geode.net.SSLParameterExtension;
import org.apache.geode.util.internal.GeodeGlossary;


/**
 * SocketCreators are built using a SocketCreatorFactory using Geode distributed-system properties.
 * They know how to properly configure sockets for TLS (SSL) communications and perform
 * handshakes. Connection-initiation uses a HostAndPort instance that is similar to an
 * InetSocketAddress.
 * <p>
 * SocketCreator also supports a client-socket-factory that is designated with the property
 * gemfire.clientSocketFactory for use in creating client->server connections.
 */
public class SocketCreator extends TcpSocketCreatorImpl {

  private static final Logger logger = LogService.getLogger();

  /**
   * flag to force always using DNS (regardless of the fact that these lookups can hang)
   */
  public static final boolean FORCE_DNS_USE =
      Boolean.getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "forceDnsUse");

  /**
   * set this to false to inhibit host name lookup
   */
  @MakeNotStatic
  public static volatile boolean resolve_dns = true;

  /**
   * set this to false to use an inet_addr in a client's ID
   */
  @MakeNotStatic
  public static volatile boolean use_client_host_name = true;

  @MakeNotStatic
  private static final ConcurrentHashMap<InetAddress, String> hostNames = new ConcurrentHashMap<>();

  /**
   * Only print this SocketCreator's config once
   */
  private boolean configShown = false;
  /**
   * Only print hostname validation disabled log once
   */
  private boolean hostnameValidationDisabledLogShown = false;


  private SSLContext sslContext;

  private final SSLConfig sslConfig;


  private ClientSocketFactory clientSocketFactory;

  /**
   * Whether to enable TCP keep alive for sockets. This boolean is controlled by the
   * gemfire.setTcpKeepAlive java system property. If not set then GemFire will enable keep-alive on
   * server->client and p2p connections.
   */
  public static final boolean ENABLE_TCP_KEEP_ALIVE =
      AdvancedSocketCreatorImpl.ENABLE_TCP_KEEP_ALIVE;

  // -------------------------------------------------------------------------
  // Static instance accessors
  // -------------------------------------------------------------------------

  /**
   * This method has migrated to LocalHostUtil but is kept in place here for
   * backward-compatibility testing.
   *
   * @deprecated use LocalHostUtil.getLocalHost()
   */
  public static InetAddress getLocalHost() throws UnknownHostException {
    return LocalHostUtil.getLocalHost();
  }


  /**
   * returns the host name for the given inet address, using a local cache of names to avoid dns
   * hits and duplicate strings
   */
  public static String getHostName(InetAddress addr) {
    String result = hostNames.get(addr);
    if (result == null) {
      result = addr.getHostName();
      hostNames.put(addr, result);
    }
    return result;
  }

  /**
   * Reset the hostNames caches
   */
  public static void resetHostNameCache() {
    hostNames.clear();
  }


  // -------------------------------------------------------------------------
  // Constructor
  // -------------------------------------------------------------------------

  /**
   * Constructs new SocketCreator instance.
   */
  public SocketCreator(final SSLConfig sslConfig) {
    this.sslConfig = sslConfig;
    initialize();
  }

  /** returns the hostname or address for this client */
  public static String getClientHostName() throws UnknownHostException {
    InetAddress hostAddr = LocalHostUtil.getLocalHost();
    return SocketCreator.use_client_host_name ? hostAddr.getCanonicalHostName()
        : hostAddr.getHostAddress();
  }

  // -------------------------------------------------------------------------
  // Initializers (change SocketCreator state)
  // -------------------------------------------------------------------------

  protected void initializeCreators() {
    clusterSocketCreator = new SCClusterSocketCreator(this);
    clientSocketCreator = new SCClientSocketCreator(this);
    advancedSocketCreator = new SCAdvancedSocketCreator(this);
  }

  /**
   * Initialize this SocketCreator.
   * <p>
   * Caller must synchronize on the SocketCreator instance.
   */
  private void initialize() {
    try {
      try {
        if (this.sslConfig.isEnabled() && getSslContext() == null) {
          sslContext = createAndConfigureSSLContext();
        }
      } catch (Exception e) {
        throw new GemFireConfigException("Error configuring GemFire ssl ", e);
      }

      // make sure TCPConduit picks up p2p properties...
      org.apache.geode.internal.tcp.TCPConduit.init();

      initializeClientSocketFactory();

    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error. We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (Error t) {
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above). However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      t.printStackTrace();
      throw t;
    } catch (RuntimeException re) {
      re.printStackTrace();
      throw re;
    }
  }

  /**
   * Creates & configures the SSLContext when SSL is enabled.
   *
   * @return new SSLContext configured using the given protocols & properties
   *
   * @throws GeneralSecurityException if security information can not be found
   * @throws IOException if information can not be loaded
   */
  private SSLContext createAndConfigureSSLContext() throws GeneralSecurityException, IOException {

    if (sslConfig.useDefaultSSLContext()) {
      return SSLContext.getDefault();
    }

    SSLContext newSSLContext = SSLUtil.getSSLContextInstance(sslConfig);
    KeyManager[] keyManagers = getKeyManagers();
    TrustManager[] trustManagers = getTrustManagers();

    newSSLContext.init(keyManagers, trustManagers, null /* use the default secure random */);
    return newSSLContext;
  }

  /**
   * Used by SystemAdmin to read the properties from console
   *
   * @param env Map in which the properties are to be read from console.
   */
  public static void readSSLProperties(Map<String, String> env) {
    readSSLProperties(env, false);
  }

  /**
   * Used to read the properties from console. AgentLauncher calls this method directly & ignores
   * gemfire.properties. SystemAdmin calls this through {@link #readSSLProperties(Map)} and does
   * NOT ignore gemfire.properties.
   *
   * @param env Map in which the properties are to be read from console.
   * @param ignoreGemFirePropsFile if <code>false</code> existing gemfire.properties file is read,
   *        if <code>true</code>, properties from gemfire.properties file are ignored.
   */
  public static void readSSLProperties(Map<String, String> env, boolean ignoreGemFirePropsFile) {
    Properties props = new Properties();
    DistributionConfigImpl.loadGemFireProperties(props, ignoreGemFirePropsFile);
    for (Map.Entry<Object, Object> ent : props.entrySet()) {
      String key = (String) ent.getKey();
      // if the value of ssl props is empty, read them from console
      if (key.startsWith(DistributionConfig.SSL_SYSTEM_PROPS_NAME)
          || key.startsWith(DistributionConfig.SYS_PROP_NAME)) {
        if (key.startsWith(DistributionConfig.SYS_PROP_NAME)) {
          key = key.substring(DistributionConfig.SYS_PROP_NAME.length());
        }
        final String value = (String) ent.getValue();
        if (value == null || value.trim().equals("")) {
          Console console = System.console();
          if (console == null) {
            throw new GemFireConfigException(
                "SSL properties are empty, but a console is not available");
          }
          String val = console.readLine("Please enter " + key + ": ");
          env.put(key, val);
        }
      }
    }
  }

  private TrustManager[] getTrustManagers()
      throws KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException {
    TrustManager[] trustManagers;

    String trustStoreType = sslConfig.getTruststoreType();
    if (StringUtils.isEmpty(trustStoreType)) {
      trustStoreType = KeyStore.getDefaultType();
    }

    KeyStore ts = KeyStore.getInstance(trustStoreType);
    String trustStorePath = sslConfig.getTruststore();
    FileInputStream fis = new FileInputStream(trustStorePath);
    String passwordString = sslConfig.getTruststorePassword();
    char[] password = null;
    if (passwordString != null) {
      if (passwordString.trim().equals("")) {
        if (!StringUtils.isEmpty(passwordString)) {
          String toDecrypt = "encrypted(" + passwordString + ")";
          passwordString = PasswordUtil.decrypt(toDecrypt);
          password = passwordString.toCharArray();
        }
      } else {
        password = passwordString.toCharArray();
      }
    }
    ts.load(fis, password);

    // default algorithm can be changed by setting property "ssl.TrustManagerFactory.algorithm" in
    // security properties
    TrustManagerFactory tmf =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tmf.init(ts);
    trustManagers = tmf.getTrustManagers();
    // follow the security tip in java doc
    if (password != null) {
      java.util.Arrays.fill(password, ' ');
    }

    return trustManagers;
  }

  private KeyManager[] getKeyManagers() throws KeyStoreException, IOException,
      NoSuchAlgorithmException, CertificateException, UnrecoverableKeyException {
    if (sslConfig.getKeystore() == null) {
      return null;
    }

    KeyManager[] keyManagers;
    String keyStoreType = sslConfig.getKeystoreType();
    if (StringUtils.isEmpty(keyStoreType)) {
      keyStoreType = KeyStore.getDefaultType();
    }
    KeyStore keyStore = KeyStore.getInstance(keyStoreType);
    String keyStoreFilePath = sslConfig.getKeystore();
    if (StringUtils.isEmpty(keyStoreFilePath)) {
      keyStoreFilePath =
          System.getProperty("user.home") + System.getProperty("file.separator") + ".keystore";
    }


    FileInputStream fileInputStream = new FileInputStream(keyStoreFilePath);
    String passwordString = sslConfig.getKeystorePassword();
    char[] password = null;
    if (passwordString != null) {
      if (passwordString.trim().equals("")) {
        String encryptedPass = System.getenv("javax.net.ssl.keyStorePassword");
        if (!StringUtils.isEmpty(encryptedPass)) {
          String toDecrypt = "encrypted(" + encryptedPass + ")";
          passwordString = PasswordUtil.decrypt(toDecrypt);
          password = passwordString.toCharArray();
        }
      } else {
        password = passwordString.toCharArray();
      }
    }
    keyStore.load(fileInputStream, password);
    // default algorithm can be changed by setting property "ssl.KeyManagerFactory.algorithm" in
    // security properties
    KeyManagerFactory keyManagerFactory =
        KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    keyManagerFactory.init(keyStore, password);
    keyManagers = keyManagerFactory.getKeyManagers();
    // follow the security tip in java doc
    if (password != null) {
      java.util.Arrays.fill(password, ' ');
    }

    KeyManager[] extendedKeyManagers = new KeyManager[keyManagers.length];

    for (int i = 0; i < keyManagers.length; i++)

    {
      extendedKeyManagers[i] = new ExtendedAliasKeyManager(keyManagers[i], sslConfig.getAlias());
    }

    return extendedKeyManagers;
  }

  /**
   * context for SSL socket factories
   */
  @VisibleForTesting
  public SSLContext getSslContext() {
    return sslContext;
  }

  /**
   * A factory used to create client <code>Sockets</code>.
   */
  public ClientSocketFactory getClientSocketFactory() {
    return clientSocketFactory;
  }

  public SSLConfig getSslConfig() {
    return sslConfig;
  }

  /**
   * ExtendedAliasKeyManager supports use of certificate aliases in distributed system
   * properties.
   */
  private static class ExtendedAliasKeyManager extends X509ExtendedKeyManager {

    private final X509ExtendedKeyManager delegate;

    private final String keyAlias;

    /**
     * Constructor.
     *
     * @param mgr The X509KeyManager used as a delegate
     * @param keyAlias The alias name of the server's keypair and supporting certificate chain
     */
    ExtendedAliasKeyManager(KeyManager mgr, String keyAlias) {
      this.delegate = (X509ExtendedKeyManager) mgr;
      this.keyAlias = keyAlias;
    }


    @Override
    public String[] getClientAliases(final String s, final Principal[] principals) {
      return delegate.getClientAliases(s, principals);
    }

    @Override
    public String chooseClientAlias(final String[] strings, final Principal[] principals,
        final Socket socket) {
      if (!StringUtils.isEmpty(this.keyAlias)) {
        return keyAlias;
      }
      return delegate.chooseClientAlias(strings, principals, socket);
    }

    @Override
    public String[] getServerAliases(final String s, final Principal[] principals) {
      return delegate.getServerAliases(s, principals);
    }

    @Override
    public String chooseServerAlias(String keyType, Principal[] issuers, Socket socket) {
      if (!StringUtils.isEmpty(this.keyAlias)) {
        PrivateKey key = this.delegate.getPrivateKey(this.keyAlias);
        return getKeyAlias(keyType, key);
      }
      return this.delegate.chooseServerAlias(keyType, issuers, socket);

    }

    @Override
    public X509Certificate[] getCertificateChain(final String s) {
      if (!StringUtils.isEmpty(this.keyAlias)) {
        return delegate.getCertificateChain(keyAlias);
      }
      return delegate.getCertificateChain(s);
    }

    @Override
    public PrivateKey getPrivateKey(final String alias) {
      return delegate.getPrivateKey(alias);
    }

    @Override
    public String chooseEngineClientAlias(String[] keyTypes, Principal[] principals,
        SSLEngine sslEngine) {
      return delegate.chooseEngineClientAlias(keyTypes, principals, sslEngine);
    }

    @Override
    public String chooseEngineServerAlias(final String keyType, final Principal[] principals,
        final SSLEngine sslEngine) {
      if (!StringUtils.isEmpty(this.keyAlias)) {
        PrivateKey key = this.delegate.getPrivateKey(this.keyAlias);
        return getKeyAlias(keyType, key);
      }
      return this.delegate.chooseEngineServerAlias(keyType, principals, sslEngine);

    }

    private String getKeyAlias(final String keyType, final PrivateKey key) {
      if (key != null) {
        if (key.getAlgorithm().equals(keyType)) {
          return this.keyAlias;
        } else {
          return null;
        }
      } else {
        return null;
      }
    }
  }

  /**
   * Returns true if this SocketCreator is configured to use SSL.
   */
  @Override
  protected boolean useSSL() {
    return this.sslConfig.isEnabled();
  }

  // -------------------------------------------------------------------------
  // Public methods
  // -------------------------------------------------------------------------

  /**
   * Returns an SSLEngine that can be used to perform TLS handshakes and communication
   */
  public SSLEngine createSSLEngine(String hostName, int port) {
    return getSslContext().createSSLEngine(hostName, port);
  }

  /**
   * @see <a
   *      href=https://docs.oracle.com/javase/8/docs/technotes/guides/security/jsse/JSSERefGuide.html#SSLENG">JSSE
   *      Reference Guide</a>
   *
   * @param socketChannel the socket's NIO channel
   * @param engine the sslEngine (see createSSLEngine)
   * @param timeout handshake timeout in milliseconds. No timeout if <= 0
   * @param clientSocket set to true if you initiated the connect(), false if you accepted it
   * @param peerNetBuffer the buffer to use in reading data fron socketChannel. This should also be
   *        used in subsequent I/O operations
   * @return The SSLEngine to be used in processing data for sending/receiving from the channel
   */
  public NioSslEngine handshakeSSLSocketChannel(SocketChannel socketChannel, SSLEngine engine,
      int timeout,
      boolean clientSocket,
      ByteBuffer peerNetBuffer,
      BufferPool bufferPool)
      throws IOException {
    engine.setUseClientMode(clientSocket);
    if (!clientSocket) {
      engine.setNeedClientAuth(sslConfig.isRequireAuth());
    }

    if (clientSocket) {
      SSLParameters modifiedParams = checkAndEnableHostnameValidation(engine.getSSLParameters());
      engine.setSSLParameters(modifiedParams);
    }
    while (!socketChannel.finishConnect()) {
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        if (!socketChannel.socket().isClosed()) {
          socketChannel.close();
        }
        throw new IOException("Interrupted while performing handshake", e);
      }
    }

    NioSslEngine nioSslEngine = new NioSslEngine(engine, bufferPool);

    boolean blocking = socketChannel.isBlocking();
    if (blocking) {
      socketChannel.configureBlocking(false);
    }

    try {
      nioSslEngine.handshake(socketChannel, timeout, peerNetBuffer);
    } catch (SSLException e) {
      if (!socketChannel.socket().isClosed()) {
        socketChannel.close();
      }
      logger.warn("SSL handshake exception", e);
      throw e;
    } catch (InterruptedException e) {
      if (!socketChannel.socket().isClosed()) {
        socketChannel.close();
      }
      throw new IOException("SSL handshake interrupted");
    } finally {
      if (blocking) {
        try {
          socketChannel.configureBlocking(true);
        } catch (IOException ignored) {
          // problem setting the socket back to blocking mode but the socket's going to be closed
        }
      }
    }
    return nioSslEngine;
  }

  private SSLParameters checkAndEnableHostnameValidation(SSLParameters sslParameters) {
    if (sslConfig.doEndpointIdentification()) {
      sslParameters.setEndpointIdentificationAlgorithm("HTTPS");
    } else {
      if (!hostnameValidationDisabledLogShown) {
        logger.info("Your SSL configuration disables hostname validation. "
            + "ssl-endpoint-identification-enabled should be set to true when SSL is enabled. "
            + "Please refer to the Apache GEODE SSL Documentation for SSL Property: ssl‑endpoint‑identification‑enabled");
        hostnameValidationDisabledLogShown = true;
      }
    }
    return sslParameters;
  }

  /**
   * Use this method to perform the SSL handshake on a newly accepted socket. Non-SSL
   * sockets are ignored by this method.
   *
   * @param timeout the number of milliseconds allowed for the handshake to complete
   */
  void handshakeIfSocketIsSSL(Socket socket, int timeout) throws IOException {
    if (!(socket instanceof SSLSocket)) {
      return;
    }
    int oldTimeout = socket.getSoTimeout();
    socket.setSoTimeout(timeout);
    SSLSocket sslSocket = (SSLSocket) socket;
    try {
      sslSocket.startHandshake();
    } catch (SSLPeerUnverifiedException ex) {
      if (this.sslConfig.isRequireAuth()) {
        logger.fatal(String.format("SSL Error in authenticating peer %s[%s].",
            socket.getInetAddress(), socket.getPort()), ex);
        throw ex;
      }
    }
    // Pre jkd11, startHandshake is throwing SocketTimeoutException.
    // in jdk 11 it is throwing SSLProtocolException with a cause of SocketTimeoutException.
    // this is to keep the exception consistent across jdk
    catch (SSLProtocolException ex) {
      if (ex.getCause() instanceof SocketTimeoutException) {
        throw (SocketTimeoutException) ex.getCause();
      } else {
        throw ex;
      }
    } finally {
      try {
        socket.setSoTimeout(oldTimeout);
      } catch (SocketException ignored) {
      }
    }
  }

  /**
   * Create a server socket with the given transport filters.<br>
   * Note: This method is outside of the
   * client/server/advanced interfaces because it references WAN classes that aren't
   * available to them.
   */
  public ServerSocket createServerSocket(int nport, int backlog, InetAddress bindAddr,
      List<GatewayTransportFilter> transportFilters, int socketBufferSize) throws IOException {
    if (transportFilters.isEmpty()) {
      return ((SCClusterSocketCreator) forCluster())
          .createServerSocket(nport, backlog, bindAddr, socketBufferSize, useSSL());
    } else {
      printConfig();
      ServerSocket result = new TransportFilterServerSocket(transportFilters);
      result.setReuseAddress(true);
      // Set the receive buffer size before binding the socket so
      // that large buffers will be allocated on accepted sockets (see
      // java.net.ServerSocket.setReceiverBufferSize javadocs)
      result.setReceiveBufferSize(socketBufferSize);
      try {
        result.bind(new InetSocketAddress(bindAddr, nport), backlog);
      } catch (BindException e) {
        BindException throwMe = new BindException(
            String.format("Failed to create server socket on %s[%s]", bindAddr, nport));
        throwMe.initCause(e);
        throw throwMe;
      }
      return result;
    }
  }


  // -------------------------------------------------------------------------
  // Private implementation methods
  // -------------------------------------------------------------------------


  /**
   * When a socket is accepted from a server socket, it should be passed to this method for SSL
   * configuration.
   */
  void configureClientSSLSocket(Socket socket, HostAndPort addr, int timeout) throws IOException {
    if (socket instanceof SSLSocket) {
      SSLSocket sslSocket = (SSLSocket) socket;

      sslSocket.setUseClientMode(true);
      sslSocket.setEnableSessionCreation(true);

      SSLParameters modifiedParams =
          checkAndEnableHostnameValidation(sslSocket.getSSLParameters());

      setServerNames(modifiedParams, addr);

      SSLParameterExtension sslParameterExtension = this.sslConfig.getSSLParameterExtension();
      if (sslParameterExtension != null) {
        modifiedParams =
            sslParameterExtension.modifySSLClientSocketParameters(modifiedParams);
      }
      sslSocket.setSSLParameters(modifiedParams);

      String[] protocols = this.sslConfig.getProtocolsAsStringArray();

      // restrict cyphers
      if (protocols != null && !"any".equalsIgnoreCase(protocols[0])) {
        sslSocket.setEnabledProtocols(protocols);
      }
      String[] ciphers = this.sslConfig.getCiphersAsStringArray();
      if (ciphers != null && !"any".equalsIgnoreCase(ciphers[0])) {
        sslSocket.setEnabledCipherSuites(ciphers);
      }

      try {
        if (timeout > 0) {
          sslSocket.setSoTimeout(timeout);
        }
        sslSocket.startHandshake();
      }
      // Pre jkd11, startHandshake is throwing SocketTimeoutException.
      // in jdk 11 it is throwing SSLProtocolException with a cause of SocketTimeoutException.
      // this is to keep the exception consistent across jdk
      catch (SSLProtocolException ex) {
        if (ex.getCause() instanceof SocketTimeoutException) {
          throw (SocketTimeoutException) ex.getCause();
        } else {
          throw ex;
        }
      } catch (SSLHandshakeException ex) {
        logger.fatal(String.format("Problem forming SSL connection to %s[%s].",
            socket.getInetAddress(), socket.getPort()), ex);
        throw ex;
      } catch (SSLPeerUnverifiedException ex) {
        if (this.sslConfig.isRequireAuth()) {
          logger.fatal("SSL authentication exception.", ex);
          throw ex;
        }
      }
    }
  }

  private void setServerNames(SSLParameters modifiedParams, HostAndPort addr) {
    List<SNIServerName> oldNames = modifiedParams.getServerNames();
    oldNames = oldNames == null ? Collections.emptyList() : oldNames;
    final List<SNIServerName> serverNames = new ArrayList<>(oldNames);

    if (serverNames.stream()
        .mapToInt(SNIServerName::getType)
        .anyMatch(type -> type == StandardConstants.SNI_HOST_NAME)) {
      // we already have a SNI hostname set. Do nothing.
      return;
    }

    serverNames.add(new SNIHostName(addr.getHostName()));
    modifiedParams.setServerNames(serverNames);
  }

  /**
   * Print current configured state to log.
   */
  void printConfig() {
    if (!configShown && logger.isDebugEnabled()) {
      configShown = true;
      StringBuilder sb = new StringBuilder();
      sb.append("SSL Configuration: \n");
      sb.append("  ssl-enabled = ").append(this.sslConfig.isEnabled()).append("\n");
      // add other options here....
      for (String key : System.getProperties().stringPropertyNames()) { // fix for 46822
        if (key.startsWith("javax.net.ssl")) {
          String possiblyRedactedValue =
              ArgumentRedactor.redactArgumentIfNecessary(key, System.getProperty(key));
          sb.append("  ").append(key).append(" = ").append(possiblyRedactedValue).append("\n");
        }
      }
      logger.debug(sb.toString());
    }
  }

  protected void initializeClientSocketFactory() {
    this.clientSocketFactory = null;
    String className =
        System.getProperty(GeodeGlossary.GEMFIRE_PREFIX + "clientSocketFactory");
    if (className != null) {
      Object o;
      try {
        Class c = ClassPathLoader.getLatest().forName(className);
        o = c.newInstance();
      } catch (Exception e) {
        // No cache exists yet, so this can't be logged.
        String s = "An unexpected exception occurred while instantiating a " + className + ": " + e;
        throw new IllegalArgumentException(s);
      }
      if (o instanceof ClientSocketFactory) {
        this.clientSocketFactory = (ClientSocketFactory) o;
      } else {
        String s = "Class \"" + className + "\" is not a ClientSocketFactory";
        throw new IllegalArgumentException(s);
      }
    }
  }

  public void initializeTransportFilterClientSocketFactory(GatewaySender sender) {
    this.clientSocketFactory = new TransportFilterSocketFactory()
        .setGatewayTransportFilters(sender.getGatewayTransportFilters());
  }
}
