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


import java.io.FileInputStream;
import java.io.IOException;
import java.net.BindException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
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
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.net.ServerSocketFactory;
import javax.net.SocketFactory;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLProtocolException;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.GemFireConfigException;
import org.apache.geode.SystemConnectException;
import org.apache.geode.SystemFailure;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.cache.wan.GatewayTransportFilter;
import org.apache.geode.distributed.ClientSocketFactory;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.ConnectionWatcher;
import org.apache.geode.internal.GfeConsoleReaderFactory;
import org.apache.geode.internal.GfeConsoleReaderFactory.GfeConsoleReader;
import org.apache.geode.internal.admin.SSLConfig;
import org.apache.geode.internal.cache.wan.TransportFilterServerSocket;
import org.apache.geode.internal.cache.wan.TransportFilterSocketFactory;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.util.ArgumentRedactor;
import org.apache.geode.internal.util.PasswordUtil;
import org.apache.geode.management.internal.SSLUtil;

/**
 * Analyze configuration data (gemfire.properties) and configure sockets accordingly for SSL.
 * <p>
 * gemfire.useSSL = (true|false) default false.<br/>
 * gemfire.ssl.debug = (true|false) default false.<br/>
 * gemfire.ssl.needClientAuth = (true|false) default true.<br/>
 * gemfire.ssl.protocols = <i>list of protocols</i><br/>
 * gemfire.ssl.ciphers = <i>list of cipher suites</i><br/>
 * <p>
 * The following may be included to configure the certificates used by the Sun Provider.
 * <p>
 * javax.net.ssl.trustStore = <i>pathname</i><br/>
 * javax.net.ssl.trustStorePassword = <i>password</i><br/>
 * javax.net.ssl.keyStore = <i>pathname</i><br/>
 * javax.net.ssl.keyStorePassword = <i>password</i><br/>
 * <p>
 * Additional properties will be set as System properties to be available as needed by other
 * provider implementations.
 */
public class SocketCreator {

  private static final Logger logger = LogService.getLogger();

  /**
   * Optional system property to enable GemFire usage of link-local addresses
   */
  private static final String USE_LINK_LOCAL_ADDRESSES_PROPERTY =
      DistributionConfig.GEMFIRE_PREFIX + "net.useLinkLocalAddresses";

  /**
   * True if GemFire should use link-local addresses
   */
  private static final boolean useLinkLocalAddresses =
      Boolean.getBoolean(USE_LINK_LOCAL_ADDRESSES_PROPERTY);

  /**
   * we cache localHost to avoid bug #40619, access-violation in native code
   */
  private static final InetAddress localHost;

  /**
   * all classes should use this variable to determine whether to use IPv4 or IPv6 addresses
   */
  @MakeNotStatic
  private static boolean useIPv6Addresses = !Boolean.getBoolean("java.net.preferIPv4Stack")
      && Boolean.getBoolean("java.net.preferIPv6Addresses");

  @MakeNotStatic
  private static final ConcurrentHashMap<InetAddress, String> hostNames = new ConcurrentHashMap<>();

  /**
   * flag to force always using DNS (regardless of the fact that these lookups can hang)
   */
  public static final boolean FORCE_DNS_USE =
      Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "forceDnsUse");

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

  /**
   * Only print this SocketCreator's config once
   */
  private boolean configShown = false;
  /**
   * Only print hostname validation disabled log once
   */
  private boolean hostnameValidationDisabledLogShown = false;


  /**
   * context for SSL socket factories
   */
  private SSLContext sslContext;

  private SSLConfig sslConfig;

  static {
    InetAddress inetAddress = null;
    try {
      inetAddress = InetAddress.getByAddress(InetAddress.getLocalHost().getAddress());
      if (inetAddress.isLoopbackAddress()) {
        InetAddress ipv4Fallback = null;
        InetAddress ipv6Fallback = null;
        // try to find a non-loopback address
        Set<InetAddress> myInterfaces = getMyAddresses();
        boolean preferIPv6 = SocketCreator.useIPv6Addresses;
        String lhName = null;
        for (InetAddress addr : myInterfaces) {
          if (addr.isLoopbackAddress() || addr.isAnyLocalAddress() || lhName != null) {
            break;
          }
          boolean ipv6 = addr instanceof Inet6Address;
          boolean ipv4 = addr instanceof Inet4Address;
          if ((preferIPv6 && ipv6) || (!preferIPv6 && ipv4)) {
            String addrName = reverseDNS(addr);
            if (inetAddress.isLoopbackAddress()) {
              inetAddress = addr;
              lhName = addrName;
            } else if (addrName != null) {
              inetAddress = addr;
              lhName = addrName;
            }
          } else {
            if (preferIPv6 && ipv4 && ipv4Fallback == null) {
              ipv4Fallback = addr;
            } else if (!preferIPv6 && ipv6 && ipv6Fallback == null) {
              ipv6Fallback = addr;
            }
          }
        }
        // vanilla Ubuntu installations will have a usable IPv6 address when
        // running as a guest OS on an IPv6-enabled machine. We also look for
        // the alternative IPv4 configuration.
        if (inetAddress.isLoopbackAddress()) {
          if (ipv4Fallback != null) {
            inetAddress = ipv4Fallback;
            SocketCreator.useIPv6Addresses = false;
          } else if (ipv6Fallback != null) {
            inetAddress = ipv6Fallback;
            SocketCreator.useIPv6Addresses = true;
          }
        }
      }
    } catch (UnknownHostException ignored) {
    }
    localHost = inetAddress;
  }

  /**
   * A factory used to create client <code>Sockets</code>.
   */
  private ClientSocketFactory clientSocketFactory;

  /**
   * Whether to enable TCP keep alive for sockets. This boolean is controlled by the
   * gemfire.setTcpKeepAlive java system property. If not set then GemFire will enable keep-alive on
   * server->client and p2p connections.
   */
  public static final boolean ENABLE_TCP_KEEP_ALIVE;

  static {
    // bug #49484 - customers want tcp/ip keep-alive turned on by default
    // to avoid dropped connections. It can be turned off by setting this
    // property to false
    String str = System.getProperty(DistributionConfig.GEMFIRE_PREFIX + "setTcpKeepAlive");
    if (str != null) {
      ENABLE_TCP_KEEP_ALIVE = Boolean.valueOf(str);
    } else {
      ENABLE_TCP_KEEP_ALIVE = true;
    }
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


  // -------------------------------------------------------------------------
  // Static instance accessors
  // -------------------------------------------------------------------------

  /**
   * All GemFire code should use this method instead of InetAddress.getLocalHost(). See bug #40619
   */
  public static InetAddress getLocalHost() throws UnknownHostException {
    if (localHost == null) {
      throw new UnknownHostException();
    }
    return localHost;
  }

  /**
   * All classes should use this instead of relying on the JRE system property
   */
  public static boolean preferIPv6Addresses() {
    return SocketCreator.useIPv6Addresses;
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
   * returns the host name for the given inet address, using a local cache of names to avoid dns
   * hits and duplicate strings
   */
  public static String getCanonicalHostName(InetAddress addr, String hostName) {
    String result = hostNames.get(addr);
    if (result == null) {
      hostNames.put(addr, hostName);
      return hostName;
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
  // Initializers (change SocketCreator state)
  // -------------------------------------------------------------------------

  /**
   * Initialize this SocketCreator.
   * <p>
   * Caller must synchronize on the SocketCreator instance.
   */
  private void initialize() {
    try {
      try {
        if (this.sslConfig.isEnabled() && sslContext == null) {
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
          GfeConsoleReader consoleReader = GfeConsoleReaderFactory.getDefaultConsoleReader();
          if (!consoleReader.isSupported()) {
            throw new GemFireConfigException(
                "SSL properties are empty, but a console is not available");
          }
          String val = consoleReader.readLine("Please enter " + key + ": ");
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

  public SSLContext getSslContext() {
    return sslContext;
  }

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

  // -------------------------------------------------------------------------
  // Public methods
  // -------------------------------------------------------------------------

  /**
   * Returns true if this SocketCreator is configured to use SSL.
   */
  public boolean useSSL() {
    return this.sslConfig.isEnabled();
  }

  /**
   * Return a ServerSocket possibly configured for SSL. SSL configuration is left up to JSSE
   * properties in java.security file.
   */
  public ServerSocket createServerSocket(int nport, int backlog) throws IOException {
    return createServerSocket(nport, backlog, null);
  }

  public ServerSocket createServerSocket(int nport, int backlog, InetAddress bindAddr,
      List<GatewayTransportFilter> transportFilters, int socketBufferSize) throws IOException {
    if (transportFilters.isEmpty()) {
      return createServerSocket(nport, backlog, bindAddr, socketBufferSize);
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

  /**
   * Return a ServerSocket possibly configured for SSL. SSL configuration is left up to JSSE
   * properties in java.security file.
   */
  public ServerSocket createServerSocket(int nport, int backlog, InetAddress bindAddr)
      throws IOException {
    return createServerSocket(nport, backlog, bindAddr, -1, sslConfig.isEnabled());
  }

  public ServerSocket createServerSocket(int nport, int backlog, InetAddress bindAddr,
      int socketBufferSize) throws IOException {
    return createServerSocket(nport, backlog, bindAddr, socketBufferSize, sslConfig.isEnabled());
  }

  private ServerSocket createServerSocket(int nport, int backlog, InetAddress bindAddr,
      int socketBufferSize, boolean sslConnection) throws IOException {
    printConfig();
    if (sslConnection) {
      if (this.sslContext == null) {
        throw new GemFireConfigException(
            "SSL not configured correctly, Please look at previous error");
      }
      ServerSocketFactory ssf = this.sslContext.getServerSocketFactory();
      SSLServerSocket serverSocket = (SSLServerSocket) ssf.createServerSocket();
      serverSocket.setReuseAddress(true);
      // If necessary, set the receive buffer size before binding the socket so
      // that large buffers will be allocated on accepted sockets (see
      // java.net.ServerSocket.setReceiverBufferSize javadocs)
      if (socketBufferSize != -1) {
        serverSocket.setReceiveBufferSize(socketBufferSize);
      }
      serverSocket.bind(new InetSocketAddress(bindAddr, nport), backlog);
      finishServerSocket(serverSocket);
      return serverSocket;
    } else {
      // log.info("Opening server socket on " + nport, new Exception("SocketCreation"));
      ServerSocket result = new ServerSocket();
      result.setReuseAddress(true);
      // If necessary, set the receive buffer size before binding the socket so
      // that large buffers will be allocated on accepted sockets (see
      // java.net.ServerSocket.setReceiverBufferSize javadocs)
      if (socketBufferSize != -1) {
        result.setReceiveBufferSize(socketBufferSize);
      }
      try {
        result.bind(new InetSocketAddress(bindAddr, nport), backlog);
      } catch (BindException e) {
        BindException throwMe =
            new BindException(String.format("Failed to create server socket on %s[%s]",
                bindAddr == null ? InetAddress.getLocalHost() : bindAddr,
                String.valueOf(nport)));
        throwMe.initCause(e);
        throw throwMe;
      }
      return result;
    }
  }

  /**
   * Creates or bind server socket to a random port selected from tcp-port-range which is same as
   * membership-port-range.
   *
   *
   * @return Returns the new server socket.
   *
   */
  public ServerSocket createServerSocketUsingPortRange(InetAddress ba, int backlog,
      boolean isBindAddress, boolean useNIO, int tcpBufferSize, int[] tcpPortRange)
      throws IOException {
    return createServerSocketUsingPortRange(ba, backlog, isBindAddress, useNIO, tcpBufferSize,
        tcpPortRange, sslConfig.isEnabled());
  }

  /**
   * Creates or bind server socket to a random port selected from tcp-port-range which is same as
   * membership-port-range.
   *
   * @param sslConnection whether to connect using SSL
   *
   * @return Returns the new server socket.
   *
   */
  public ServerSocket createServerSocketUsingPortRange(InetAddress ba, int backlog,
      boolean isBindAddress, boolean useNIO, int tcpBufferSize, int[] tcpPortRange,
      boolean sslConnection) throws IOException {

    // Get a random port from range.
    int startingPort = tcpPortRange[0]
        + ThreadLocalRandom.current().nextInt(tcpPortRange[1] - tcpPortRange[0] + 1);
    int localPort = startingPort;
    int portLimit = tcpPortRange[1];

    while (true) {
      if (localPort > portLimit) {
        if (startingPort != 0) {
          localPort = tcpPortRange[0];
          portLimit = startingPort - 1;
          startingPort = 0;
        } else {
          throw new SystemConnectException(
              "Unable to find a free port in the membership-port-range");
        }
      }
      ServerSocket socket = null;
      try {
        if (useNIO) {
          ServerSocketChannel channel = ServerSocketChannel.open();
          socket = channel.socket();

          InetSocketAddress address = new InetSocketAddress(isBindAddress ? ba : null, localPort);
          socket.bind(address, backlog);
        } else {
          socket = this.createServerSocket(localPort, backlog, isBindAddress ? ba : null,
              tcpBufferSize, sslConnection);
        }
        return socket;
      } catch (java.net.SocketException ex) {
        if (socket != null && !socket.isClosed()) {
          socket.close();
        }
        localPort++;
      }
    }
  }

  /**
   * Return a client socket. This method is used by client/server clients.
   */
  public Socket connectForClient(String host, int port, int timeout) throws IOException {
    return connect(InetAddress.getByName(host), port, timeout, null, true, -1);
  }

  /**
   * Return a client socket. This method is used by client/server clients.
   */
  public Socket connectForClient(String host, int port, int timeout, int socketBufferSize)
      throws IOException {
    return connect(InetAddress.getByName(host), port, timeout, null, true, socketBufferSize);
  }

  /**
   * Return a client socket. This method is used by peers.
   */
  public Socket connectForServer(InetAddress inetadd, int port) throws IOException {
    return connect(inetadd, port, 0, null, false, -1);
  }

  /**
   * Return a client socket, timing out if unable to connect and timeout > 0 (millis). The parameter
   * <i>timeout</i> is ignored if SSL is being used, as there is no timeout argument in the ssl
   * socket factory
   */
  public Socket connect(InetAddress inetadd, int port, int timeout,
      ConnectionWatcher optionalWatcher, boolean clientSide) throws IOException {
    return connect(inetadd, port, timeout, optionalWatcher, clientSide, -1);
  }

  /**
   * Return a client socket, timing out if unable to connect and timeout > 0 (millis). The parameter
   * <i>timeout</i> is ignored if SSL is being used, as there is no timeout argument in the ssl
   * socket factory
   */
  public Socket connect(InetAddress inetadd, int port, int timeout,
      ConnectionWatcher optionalWatcher, boolean clientSide, int socketBufferSize)
      throws IOException {
    return connect(inetadd, port, timeout, optionalWatcher, clientSide, socketBufferSize,
        sslConfig.isEnabled());
  }

  /**
   * Return a client socket, timing out if unable to connect and timeout > 0 (millis). The parameter
   * <i>timeout</i> is ignored if SSL is being used, as there is no timeout argument in the ssl
   * socket factory
   */
  public Socket connect(InetAddress inetadd, int port, int timeout,
      ConnectionWatcher optionalWatcher, boolean clientSide, int socketBufferSize,
      boolean sslConnection) throws IOException {
    Socket socket = null;
    SocketAddress sockaddr = new InetSocketAddress(inetadd, port);
    printConfig();
    try {
      if (sslConnection) {
        if (this.sslContext == null) {
          throw new GemFireConfigException(
              "SSL not configured correctly, Please look at previous error");
        }
        SocketFactory sf = this.sslContext.getSocketFactory();
        socket = sf.createSocket();

        // Optionally enable SO_KEEPALIVE in the OS network protocol.
        socket.setKeepAlive(ENABLE_TCP_KEEP_ALIVE);

        // If necessary, set the receive buffer size before connecting the
        // socket so that large buffers will be allocated on accepted sockets
        // (see java.net.Socket.setReceiverBufferSize javadocs for details)
        if (socketBufferSize != -1) {
          socket.setReceiveBufferSize(socketBufferSize);
        }

        if (optionalWatcher != null) {
          optionalWatcher.beforeConnect(socket);
        }
        socket.connect(sockaddr, Math.max(timeout, 0));
        configureClientSSLSocket(socket, timeout);
        return socket;
      } else {
        if (clientSide && this.clientSocketFactory != null) {
          socket = this.clientSocketFactory.createSocket(inetadd, port);
        } else {
          socket = new Socket();

          // Optionally enable SO_KEEPALIVE in the OS network protocol.
          socket.setKeepAlive(ENABLE_TCP_KEEP_ALIVE);

          // If necessary, set the receive buffer size before connecting the
          // socket so that large buffers will be allocated on accepted sockets
          // (see java.net.Socket.setReceiverBufferSize javadocs for details)
          if (socketBufferSize != -1) {
            socket.setReceiveBufferSize(socketBufferSize);
          }

          if (optionalWatcher != null) {
            optionalWatcher.beforeConnect(socket);
          }
          socket.connect(sockaddr, Math.max(timeout, 0));
        }
        return socket;
      }
    } finally {
      if (optionalWatcher != null) {
        optionalWatcher.afterConnect(socket);
      }
    }
  }

  /**
   * Returns an SSLEngine that can be used to perform TLS handshakes and communication
   */
  public SSLEngine createSSLEngine(String hostName, int port) {
    return sslContext.createSSLEngine(hostName, port);
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

  /**
   * Use this method to perform the SSL handshake on a newly accepted socket. Non-SSL
   * sockets are ignored by this method.
   *
   * @param timeout the number of milliseconds allowed for the handshake to complete
   */
  public void handshakeIfSocketIsSSL(Socket socket, int timeout) throws IOException {
    if (socket instanceof SSLSocket) {
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
  }

  // -------------------------------------------------------------------------
  // Private implementation methods
  // -------------------------------------------------------------------------

  /**
   * Configure the SSLServerSocket based on this SocketCreator's settings.
   */
  private void finishServerSocket(SSLServerSocket serverSocket) {
    serverSocket.setUseClientMode(false);
    if (this.sslConfig.isRequireAuth()) {
      // serverSocket.setWantClientAuth( true );
      serverSocket.setNeedClientAuth(true);
    }
    serverSocket.setEnableSessionCreation(true);

    // restrict protocols
    String[] protocols = this.sslConfig.getProtocolsAsStringArray();
    if (!"any".equalsIgnoreCase(protocols[0])) {
      serverSocket.setEnabledProtocols(protocols);
    }
    // restrict ciphers
    String[] ciphers = this.sslConfig.getCiphersAsStringArray();
    if (!"any".equalsIgnoreCase(ciphers[0])) {
      serverSocket.setEnabledCipherSuites(ciphers);
    }
  }

  /**
   * When a socket is accepted from a server socket, it should be passed to this method for SSL
   * configuration.
   */
  private void configureClientSSLSocket(Socket socket, int timeout) throws IOException {
    if (socket instanceof SSLSocket) {
      SSLSocket sslSocket = (SSLSocket) socket;

      sslSocket.setUseClientMode(true);
      sslSocket.setEnableSessionCreation(true);

      if (sslConfig.doEndpointIdentification()) {
        SSLParameters sslParameters = sslSocket.getSSLParameters();
        sslParameters.setEndpointIdentificationAlgorithm("HTTPS");
        sslSocket.setSSLParameters(sslParameters);
      } else {
        if (!hostnameValidationDisabledLogShown) {
          logger.info("Your SSL configuration disables hostname validation. "
              + "ssl-endpoint-identification-enabled should be set to true when SSL is enabled. "
              + "Please refer to the Apache GEODE SSL Documentation for SSL Property: ssl‑endpoint‑identification‑enabled");
          hostnameValidationDisabledLogShown = true;
        }
      }

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

  /**
   * Print current configured state to log.
   */
  private void printConfig() {
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
        System.getProperty(DistributionConfig.GEMFIRE_PREFIX + "clientSocketFactory");
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

  /**
   * returns a set of the non-loopback InetAddresses for this machine
   */
  public static Set<InetAddress> getMyAddresses() {
    Set<InetAddress> result = new HashSet<>();
    Set<InetAddress> locals = new HashSet<>();
    Enumeration<NetworkInterface> interfaces;
    try {
      interfaces = NetworkInterface.getNetworkInterfaces();
    } catch (SocketException e) {
      throw new IllegalArgumentException(
          "Unable to examine network interfaces",
          e);
    }
    while (interfaces.hasMoreElements()) {
      NetworkInterface face = interfaces.nextElement();
      boolean faceIsUp = false;
      try {
        faceIsUp = face.isUp();
      } catch (SocketException e) {
        InternalDistributedSystem ids = InternalDistributedSystem.getAnyInstance();
        if (ids != null) {
          logger.info("Failed to check if network interface is up. Skipping {}", face, e);
        }
      }
      if (faceIsUp) {
        Enumeration<InetAddress> addrs = face.getInetAddresses();
        while (addrs.hasMoreElements()) {
          InetAddress addr = addrs.nextElement();
          if (addr.isLoopbackAddress() || addr.isAnyLocalAddress()
              || (!useLinkLocalAddresses && addr.isLinkLocalAddress())) {
            locals.add(addr);
          } else {
            result.add(addr);
          }
        } // while
      }
    } // while
    // fix for bug #42427 - allow product to run on a standalone box by using
    // local addresses if there are no non-local addresses available
    if (result.size() == 0) {
      return locals;
    } else {
      return result;
    }
  }

  /**
   * This method uses JNDI to look up an address in DNS and return its name
   *
   *
   * @return the host name associated with the address or null if lookup isn't possible or there is
   *         no host name for this address
   */
  private static String reverseDNS(InetAddress addr) {
    byte[] addrBytes = addr.getAddress();
    // reverse the address suitable for reverse lookup
    StringBuilder lookup = new StringBuilder();
    for (int index = addrBytes.length - 1; index >= 0; index--) {
      lookup.append(addrBytes[index] & 0xff).append('.');
    }
    lookup.append("in-addr.arpa");

    try {
      Hashtable<String, String> env = new Hashtable<>();
      env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.dns.DnsContextFactory");
      DirContext ctx = new InitialDirContext(env);
      Attributes attrs = ctx.getAttributes(lookup.toString(), new String[] {"PTR"});
      for (NamingEnumeration ae = attrs.getAll(); ae.hasMoreElements();) {
        Attribute attr = (Attribute) ae.next();
        for (Enumeration vals = attr.getAll(); vals.hasMoreElements();) {
          Object elem = vals.nextElement();
          if ("PTR".equals(attr.getID()) && elem != null) {
            return elem.toString();
          }
        }
      }
      ctx.close();
    } catch (Exception e) {
      // ignored
    }
    return null;
  }

  /**
   * Returns true if host matches the LOCALHOST.
   */
  public static boolean isLocalHost(Object host) {
    if (host instanceof InetAddress) {
      InetAddress inetAddress = (InetAddress) host;
      if (isLocalHost(inetAddress)) {
        return true;
      } else if (inetAddress.isLoopbackAddress()) {
        return true;
      } else {
        try {
          Enumeration en = NetworkInterface.getNetworkInterfaces();
          while (en.hasMoreElements()) {
            NetworkInterface i = (NetworkInterface) en.nextElement();
            for (Enumeration en2 = i.getInetAddresses(); en2.hasMoreElements();) {
              InetAddress addr = (InetAddress) en2.nextElement();
              if (inetAddress.equals(addr)) {
                return true;
              }
            }
          }
          return false;
        } catch (SocketException e) {
          throw new IllegalArgumentException("Unable to query network interface", e);
        }
      }
    } else {
      return isLocalHost((Object) toInetAddress(host.toString()));
    }
  }

  private static boolean isLocalHost(InetAddress host) {
    try {
      return SocketCreator.getLocalHost().equals(host);
    } catch (UnknownHostException ignored) {
      return false;
    }
  }

  /**
   * Converts the string host to an instance of InetAddress. Returns null if the string is empty.
   * Fails Assertion if the conversion would result in <code>java.lang.UnknownHostException</code>.
   * <p>
   * Any leading slashes on host will be ignored.
   *
   * @param host string version the InetAddress
   *
   * @return the host converted to InetAddress instance
   */
  public static InetAddress toInetAddress(String host) {
    if (host == null || host.length() == 0) {
      return null;
    }
    try {
      final int index = host.indexOf("/");
      if (index > -1) {
        return InetAddress.getByName(host.substring(index + 1));
      } else {
        return InetAddress.getByName(host);
      }
    } catch (java.net.UnknownHostException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }
}
