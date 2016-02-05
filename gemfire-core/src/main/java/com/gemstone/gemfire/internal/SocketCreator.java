/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
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
import java.net.UnknownHostException;
import java.nio.channels.ServerSocketChannel;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Vector;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.net.ServerSocketFactory;
import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;

import com.gemstone.gemfire.GemFireConfigException;
import com.gemstone.gemfire.SystemConnectException;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.admin.internal.InetAddressUtil;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.cache.wan.GatewayTransportFilter;
import com.gemstone.gemfire.distributed.ClientSocketFactory;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionConfigImpl;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.GfeConsoleReaderFactory.GfeConsoleReader;
import com.gemstone.gemfire.internal.cache.wan.TransportFilterServerSocket;
import com.gemstone.gemfire.internal.cache.wan.TransportFilterSocketFactory;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.util.PasswordUtil;

import java.util.*;

import javax.net.ssl.*;

import org.apache.logging.log4j.Logger;

/**
 * Analyze configuration data (gemfire.properties) and configure sockets
 * accordingly for SSL.
 * <p>
 * gemfire.useSSL = (true|false) default false.<br/>
 * gemfire.ssl.debug = (true|false) default false.<br/>
 * gemfire.ssl.needClientAuth = (true|false) default true.<br/>
 * gemfire.ssl.protocols = <i>list of protocols</i><br/>
 * gemfire.ssl.ciphers = <i>list of cipher suites</i><br/>
 * <p>
 * The following may be included to configure the certificates used by the
 * Sun Provider.
 * <p>
 * javax.net.ssl.trustStore = <i>pathname</i><br/>
 * javax.net.ssl.trustStorePassword = <i>password</i><br/>
 * javax.net.ssl.keyStore = <i>pathname</i><br/>
 * javax.net.ssl.keyStorePassword = <i>password</i><br/>
 * <p>
 * Additional properties will be set as System properties to be available
 * as needed by other provider implementations.
 */
public class SocketCreator {

  private static final Logger logger = LogService.getLogger();
  
  /** Optional system property to enable GemFire usage of link-local addresses */
  public static final String USE_LINK_LOCAL_ADDRESSES_PROPERTY = 
      "gemfire.net.useLinkLocalAddresses";
  
  /** True if GemFire should use link-local addresses */
  private static final boolean useLinkLocalAddresses = 
      Boolean.getBoolean(USE_LINK_LOCAL_ADDRESSES_PROPERTY);
  
  /** we cache localHost to avoid bug #40619, access-violation in native code */
  private static final InetAddress localHost;
  
  /** all classes should use this variable to determine whether to use IPv4 or IPv6 addresses */
  private static boolean useIPv6Addresses = !Boolean.getBoolean("java.net.preferIPv4Stack") &&
  		Boolean.getBoolean("java.net.preferIPv6Addresses");
  
  private static final Map<InetAddress, String> hostNames = new HashMap<>();
  
  /** flag to force always using DNS (regardless of the fact that these lookups can hang) */
  public static final boolean FORCE_DNS_USE = Boolean.getBoolean("gemfire.forceDnsUse");
  
  /** set this to false to inhibit host name lookup */
  public static volatile boolean resolve_dns = true;

  /** The default instance for use in GemFire socket creation */
  private static SocketCreator DEFAULT_INSTANCE = new SocketCreator();
  
  /**
   * Allow unlimited concurrent reads (uses of SocketCreator).  Re-initializing 
   * SocketCreator requires the write lock which will block out all reads until 
   * it's done.  If this causes performance loss, removal of ReadWriteLock 
   * should only impact the Console or Admin APIs.
   */
  //private final ReadWriteLock rw = new ReentrantReadWriteLock();
  
  /** True if this SocketCreator has been initialized and is ready to use */
  private boolean ready = false;
  
  /** True if configured to use SSL */
  private boolean useSSL;
  
  /** True if configured to require client authentication */
  private boolean needClientAuth;
  
  /** Space-delimited list of SSL protocols to use, 'any' allows any */
  private String[] protocols;
  
  /** Space-delimited list of SSL ciphers to use, 'any' allows any */
  private String[] ciphers;
  
  /** Only print this SocketCreator's config once */
  private boolean configShown = false;

  /** context for SSL socket factories */
  private SSLContext sslContext;
  
  static {
    InetAddress lh = null;
    try {
      lh = InetAddress.getLocalHost();
      if (lh.isLoopbackAddress()) {
        InetAddress ipv4Fallback = null;
        InetAddress ipv6Fallback = null;
        // try to find a non-loopback address
        Set myInterfaces = getMyAddresses();
        boolean preferIPv6 = SocketCreator.useIPv6Addresses;
        String lhName = null;
        for (Iterator<InetAddress> it = myInterfaces.iterator(); lhName == null && it.hasNext(); ) {
          InetAddress addr = it.next();
          if (addr.isLoopbackAddress() || addr.isAnyLocalAddress()) {
            break;
          }
          boolean ipv6 = addr instanceof Inet6Address;
          boolean ipv4 = addr instanceof Inet4Address;
          if ( (preferIPv6 && ipv6)
              || (!preferIPv6 && ipv4) ) {
            String addrName = reverseDNS(addr);
            if (lh.isLoopbackAddress()) {
              lh = addr;
              lhName = addrName;
            } else if (addrName != null) {
              lh = addr;
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
        // running as a guest OS on an IPv6-enabled machine.  We also look for
        // the alternative IPv4 configuration.
        if (lh.isLoopbackAddress()) {
          if (ipv4Fallback != null) {
            lh = ipv4Fallback;
            SocketCreator.useIPv6Addresses = false;
          } else if (ipv6Fallback != null) {
            lh = ipv6Fallback;
            SocketCreator.useIPv6Addresses = true;
          }
        }
      }
    } catch (UnknownHostException e) {
    }
    localHost = lh;
  }
  
  /** A factory used to create client <code>Sockets</code>. */
  private ClientSocketFactory clientSocketFactory;
  
  /**
   * Whether to enable TCP keep alive for sockets. This boolean is controlled by
   * the gemfire.setTcpKeepAlive java system property.  If not set then GemFire
   * will enable keep-alive on server->client and p2p connections.
   */
  public static final boolean ENABLE_TCP_KEEP_ALIVE;
  
  
  
  static {
    // bug #49484 - customers want tcp/ip keep-alive turned on by default
    // to avoid dropped connections.  It can be turned off by setting this
    // property to false
    String str = System.getProperty("gemfire.setTcpKeepAlive");
    if (str != null) {
      ENABLE_TCP_KEEP_ALIVE = Boolean.getBoolean("gemfire.setTcpKeepAlive");
    } else {
      ENABLE_TCP_KEEP_ALIVE = true;
    }
  }

  // -------------------------------------------------------------------------
  //   Constructor
  // -------------------------------------------------------------------------
  
  /** Constructs new SocketCreator instance. */
  private SocketCreator() {}
  
  // -------------------------------------------------------------------------
  //   Static instance accessors
  // -------------------------------------------------------------------------
  
  /** 
   * Returns the default instance for use in GemFire socket creation. 
   * <p>
   * If not already initialized, the default instance of SocketCreator will be 
   * initialized using defaults in {@link 
   * com.gemstone.gemfire.distributed.internal.DistributionConfig}. If any 
   * values are specified in System properties, those values will be used to 
   * override the defaults.
   * <p>
   * Synchronizes on the DEFAULT_INSTANCE.
   */
  public static SocketCreator getDefaultInstance() {
    synchronized (DEFAULT_INSTANCE) {
      if (!DEFAULT_INSTANCE.ready) {
        DEFAULT_INSTANCE.initialize();
      }
    }
    return DEFAULT_INSTANCE;
  }
  
  /**
   * Returns the default instance for use in GemFire socket creation after
   * initializing it using the DistributionConfig.
   * <p>
   * This will reinitialize the SocketCreator if it was previously initialized.
   * <p>
   * Synchronizes on the DEFAULT_INSTANCE.
   */
  public static SocketCreator getDefaultInstance(DistributionConfig config) {
    synchronized (DEFAULT_INSTANCE) {
      DEFAULT_INSTANCE.initialize(config);
    }
    return DEFAULT_INSTANCE;
  }
  
  /**
   * Returns the default instance for use in GemFire socket creation after
   * initializing it using defaults in {@link 
   * com.gemstone.gemfire.distributed.internal.DistributionConfig}. If any 
   * values are specified in the provided properties or in System properties,
   * those values will be used to override the defaults.
   * <p>
   * This will reinitialize the SocketCreator if it was previously initialized.
   * <p>
   * Call will synchronize on the DEFAULT_INSTANCE.
   */
  public static SocketCreator getDefaultInstance(Properties props) {
    return getDefaultInstance(new DistributionConfigImpl(props));
  }
  
  /** 
   * Create and initialize a new non-default instance of SocketCreator. 
   * <p>
   * Synchronizes on the new instance.
   *
   * @param useSSL          true if ssl is to be enabled
   * @param needClientAuth  true if client authentication is required
   * @param protocols       space-delimited list of ssl protocols to use
   * @param ciphers         space-delimited list of ssl ciphers to use
   * @param sysProps        vendor properties to be set as System properties
   */
  public static SocketCreator createNonDefaultInstance(boolean useSSL,
                                                       boolean needClientAuth,
                                                       String protocols,
                                                       String ciphers,
                                                       Properties sysProps) {
    SocketCreator sc = new SocketCreator();
    synchronized (sc) {
      sc.initialize(useSSL, needClientAuth, readArray(protocols), readArray(ciphers), sysProps);
    }
    return sc;
  }
  
  /**
   * All GemFire code should use this method instead of
   * InetAddress.getLocalHost().  See bug #40619
   */
  public static InetAddress getLocalHost() throws UnknownHostException {
    if (localHost == null) {
      throw new UnknownHostException();
    }
    return localHost;
  }
  
  /** All classes should use this instead of relying on the JRE system property */
  public static boolean preferIPv6Addresses() {
    return SocketCreator.useIPv6Addresses;
  }
  
  /**
   * returns the host name for the given inet address, using a local cache
   * of names to avoid dns hits and duplicate strings
   */
  public static synchronized String getHostName(InetAddress addr) {
    String result = (String)hostNames.get(addr);
    if (result == null) {
      result = addr.getHostName();
      hostNames.put(addr, result);
    }
    return result;
  }
  
  /**
   * returns the host name for the given inet address, using a local cache
   * of names to avoid dns hits and duplicate strings
   */
  public static synchronized String getCanonicalHostName(InetAddress addr, String hostName) {
    String result = (String)hostNames.get(addr);
    if (result == null) {
      hostNames.put(addr, hostName);
      return hostName;
    }
    return result;
  }
  
  /**
   * Reset the hostNames caches
   */
  public static synchronized void resetHostNameCache() {
    hostNames.clear();
  }
  
  // -------------------------------------------------------------------------
  //   Initializers (change SocketCreator state)
  // -------------------------------------------------------------------------
  
  /**
   * Initialize this SocketCreator. 
   * <p>
   * Caller must synchronize on the SocketCreator instance.
   *
   * @param useSSL          true if ssl is to be enabled
   * @param needClientAuth  true if client authentication is required
   * @param protocols       array of ssl protocols to use
   * @param ciphers         array of ssl ciphers to use
   * @param props        vendor properties passed in through gfsecurity.properties
   */
  @SuppressWarnings("hiding")
  private void initialize(boolean useSSL,
                          boolean needClientAuth,
                          String[] protocols,
                          String[] ciphers,
                          Properties props) {
    Assert.assertHoldsLock(this, true); 
    try {
//       rw.writeLock().lockInterruptibly();
//       try {
        this.useSSL = useSSL;
        this.needClientAuth = needClientAuth;
        
        this.protocols = protocols;
        this.ciphers = ciphers;
        
        if (this == DEFAULT_INSTANCE) {
          // set p2p values...
          if (this.useSSL) {
            System.setProperty( "p2p.useSSL", "true" );
            System.setProperty( "p2p.oldIO", "true" );
            System.setProperty( "p2p.nodirectBuffers", "true" );
            
            try {
              if (sslContext == null) {
                sslContext = createAndConfigureSSLContext(protocols, props);
                SSLContext.setDefault(sslContext);
              }
            } catch (Exception e) {
               throw new GemFireConfigException("Error configuring GemFire ssl ",e);
            }
          }
          else {
            System.setProperty( "p2p.useSSL", "false" );
          }
          // make sure TCPConduit picks up p2p properties...
          com.gemstone.gemfire.internal.tcp.TCPConduit.init();
        } else if (this.useSSL && sslContext == null) {
          try {
            sslContext = createAndConfigureSSLContext(protocols, props);
          } catch (Exception e) {
            throw new GemFireConfigException("Error configuring GemFire ssl ",e);
          }
        }
        
        initializeClientSocketFactory();
        this.ready = true;
//       }
//       finally {
//         rw.writeLock().unlock();
//       }
    } 
     catch (VirtualMachineError err) {
       SystemFailure.initiateFailure(err);
       // If this ever returns, rethrow the error.  We're poisoned
       // now, so don't let this thread continue.
       throw err;
     }
    catch ( Error t ) {
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above).  However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      t.printStackTrace();
      throw t;
    } 
    catch ( RuntimeException re ) {
      re.printStackTrace();
      throw re;
    }
  }
  
  /**
   * Creates & configures the SSLContext when SSL is enabled.
   * 
   * @param protocolNames
   *          valid SSL protocols for this connection
   * @param props
   *          vendor properties passed in through gfsecurity.properties
   * @return new SSLContext configured using the given protocols & properties
   * 
   * @throws GeneralSecurityException
   *           if security information can not be found
   * @throws IOException
   *           if information can not be loaded
   */
  private SSLContext createAndConfigureSSLContext(String[] protocolNames, Properties props) 
      throws GeneralSecurityException, IOException {

    SSLContext     newSSLContext = getSSLContextInstance(protocolNames);
    KeyManager[]   keyManagers   = getKeyManagers(props);
    TrustManager[] trustManagers = getTrustManagers(props);
    
    newSSLContext.init(keyManagers, trustManagers, null /* use the default secure random*/);
    return newSSLContext;
  }

  /**
   * Used by CacheServerLauncher and SystemAdmin to read the properties from
   * console
   * 
   * @param env
   *          Map in which the properties are to be read from console.
   */
  public static void readSSLProperties(Map<String, String> env) {
    readSSLProperties(env, false);
  }

  /**
   * Used to read the properties from console. AgentLauncher calls this method
   * directly & ignores gemfire.properties. CacheServerLauncher and SystemAdmin
   * call this through {@link #readSSLProperties(Map)} and do NOT ignore
   * gemfire.properties.
   * 
   * @param env
   *          Map in which the properties are to be read from console.
   * @param ignoreGemFirePropsFile
   *          if <code>false</code> existing gemfire.properties file is read, if
   *          <code>true</code>, properties from gemfire.properties file are
   *          ignored.
   */
  public static void readSSLProperties(Map<String, String> env, 
      boolean ignoreGemFirePropsFile) {
    Properties props = new Properties();
    DistributionConfigImpl.loadGemFireProperties(props, ignoreGemFirePropsFile);
    for (Object entry : props.entrySet()) {
      Map.Entry<String, String> ent = (Map.Entry<String, String>)entry;
      // if the value of ssl props is empty, read them from console
      if (ent.getKey().startsWith(DistributionConfig.SSL_SYSTEM_PROPS_NAME)
          || ent.getKey().startsWith(DistributionConfig.SYS_PROP_NAME)) {
        String key = ent.getKey();
        if (key.startsWith(DistributionConfig.SYS_PROP_NAME)) {
          key = key.substring(DistributionConfig.SYS_PROP_NAME.length());
        }
        if (ent.getValue() == null || ent.getValue().trim().equals("")) {
          GfeConsoleReader consoleReader = GfeConsoleReaderFactory.getDefaultConsoleReader();
          if (!consoleReader.isSupported()) {
            throw new GemFireConfigException("SSL properties are empty, but a console is not available");
          }
          if (key.toLowerCase().contains("password")) {
            char[] password = consoleReader.readPassword("Please enter "+key+": ");
            env.put(key, PasswordUtil.encrypt(new String(password), false));
          } else {
            String val = consoleReader.readLine("Please enter "+key+": ");
            env.put(key, val);
          }
          
        }
      }
    }
  }
  
  private static SSLContext getSSLContextInstance(String[] protocols) {
    SSLContext c = null;
    if (protocols != null && protocols.length > 0) {
      for (String protocol : protocols) {
        if (!protocol.equals("any")) {
          try {
            c = SSLContext.getInstance(protocol);
            break;
          } catch (NoSuchAlgorithmException e) {
            // continue
          }
        }
      }
    }
    if (c != null) {
      return c;
    }
    // lookup known algorithms
    String[] knownAlgorithms = {"SSL", "SSLv2", "SSLv3", "TLS", "TLSv1", "TLSv1.1"};
    for (String algo : knownAlgorithms) {
      try {
        c = SSLContext.getInstance(algo);
        break;
      } catch (NoSuchAlgorithmException e) {
        // continue
      }
    }
    return c;
  }

  private TrustManager[] getTrustManagers(Properties sysProps)
      throws KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException {
    TrustManager[] trustManagers = null;
    String trustStoreType = sysProps.getProperty("javax.net.ssl.trustStoreType");
    GfeConsoleReader consoleReader = GfeConsoleReaderFactory.getDefaultConsoleReader();
    if (trustStoreType == null) {
      trustStoreType = System.getProperty("javax.net.ssl.trustStoreType", KeyStore.getDefaultType());
    } else if (trustStoreType.trim().equals("")) {
      //read from console, default on empty
      if (consoleReader.isSupported()) {
        trustStoreType = consoleReader.readLine("Please enter the trustStoreType (javax.net.ssl.trustStoreType) : ");
      }
      if (isEmpty(trustStoreType)) {
        trustStoreType = KeyStore.getDefaultType();
      }
    }
    KeyStore ts = KeyStore.getInstance(trustStoreType);
    String trustStorePath = System.getProperty("javax.net.ssl.trustStore");
    if (trustStorePath == null) {
      trustStorePath = sysProps.getProperty("javax.net.ssl.trustStore");
    }
    if (trustStorePath != null) {
      if (trustStorePath.trim().equals("")) {
        trustStorePath = System.getenv("javax.net.ssl.trustStore");
        //read from console
        if (isEmpty(trustStorePath) && consoleReader.isSupported()) {
          trustStorePath = consoleReader.readLine("Please enter the trustStore location (javax.net.ssl.trustStore) : ");
        }
      }
      FileInputStream fis = new FileInputStream(trustStorePath);
      String passwordString = System.getProperty("javax.net.ssl.trustStorePassword");
      if (passwordString == null) {
        passwordString = sysProps.getProperty("javax.net.ssl.trustStorePassword");
      }
      char [] password = null;
      if (passwordString != null) {
        if (passwordString.trim().equals("")) {
          String encryptedPass = System.getenv("javax.net.ssl.trustStorePassword");
          if (!isEmpty(encryptedPass)) {
            String toDecrypt = "encrypted(" + encryptedPass + ")";
            passwordString = PasswordUtil.decrypt(toDecrypt);
            password = passwordString.toCharArray();
          }
          //read from the console
          if (isEmpty(passwordString) && consoleReader.isSupported()) {
            password = consoleReader.readPassword("Please enter password for trustStore (javax.net.ssl.trustStorePassword) : ");
          }
        } else {
          password = passwordString.toCharArray();
        }
      }
      ts.load(fis, password);
      
      // default algorithm can be changed by setting property "ssl.TrustManagerFactory.algorithm" in security properties
      TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      tmf.init(ts);
      trustManagers = tmf.getTrustManagers();
      // follow the security tip in java doc
      if (password != null) {
        java.util.Arrays.fill(password, ' ');
      }
    }
    return trustManagers;
  }

  private KeyManager[] getKeyManagers(Properties sysProps)
      throws KeyStoreException, FileNotFoundException, IOException,
      NoSuchAlgorithmException, CertificateException, UnrecoverableKeyException {
    KeyManager[] keyManagers = null;
    String keyStoreType = sysProps.getProperty("javax.net.ssl.keyStoreType");
    GfeConsoleReader consoleReader = GfeConsoleReaderFactory.getDefaultConsoleReader();
    if (keyStoreType == null) {
      keyStoreType = System.getProperty("javax.net.ssl.keyStoreType", KeyStore.getDefaultType());
    } else if (keyStoreType.trim().equals("")) {
      // read from console, default on empty
      if (consoleReader.isSupported()) {
        keyStoreType = consoleReader.readLine("Please enter the keyStoreType (javax.net.ssl.keyStoreType) : ");
      }
      if (isEmpty(keyStoreType)) {
        keyStoreType = KeyStore.getDefaultType();
      }
    }
    KeyStore ks = KeyStore.getInstance(keyStoreType);
    String keyStoreFilePath = sysProps.getProperty("javax.net.ssl.keyStore");
    if (keyStoreFilePath == null) {
      keyStoreFilePath = System.getProperty("javax.net.ssl.keyStore");
    }
    if (keyStoreFilePath != null) {
      if (keyStoreFilePath.trim().equals("")) {
        keyStoreFilePath = System.getenv("javax.net.ssl.keyStore");
        //read from console
        if (isEmpty(keyStoreFilePath) && consoleReader.isSupported()) {
          keyStoreFilePath = consoleReader.readLine("Please enter the keyStore location (javax.net.ssl.keyStore) : ");
        }
        if (isEmpty(keyStoreFilePath)) {
          keyStoreFilePath = System.getProperty("user.home") + System.getProperty("file.separator") + ".keystore";
        }
      }
      FileInputStream fis = null;
      fis = new FileInputStream(keyStoreFilePath);
      String passwordString = sysProps.getProperty("javax.net.ssl.keyStorePassword");
      if (passwordString == null) {
        passwordString = System.getProperty("javax.net.ssl.keyStorePassword");
      }
      char [] password = null;
      if (passwordString != null) {
        if (passwordString.trim().equals("")) {
          String encryptedPass = System.getenv("javax.net.ssl.keyStorePassword");
          if (!isEmpty(encryptedPass)) {
            String toDecrypt = "encrypted(" + encryptedPass + ")";
            passwordString = PasswordUtil.decrypt(toDecrypt);
            password = passwordString.toCharArray();
          }
          //read from the console
          if (isEmpty(passwordString) && consoleReader != null) {
            password = consoleReader.readPassword("Please enter password for keyStore (javax.net.ssl.keyStorePassword) : ");
          }
        } else {
          password = passwordString.toCharArray();
        }
      }
      ks.load(fis, password);
      // default algorithm can be changed by setting property "ssl.KeyManagerFactory.algorithm" in security properties
      KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      kmf.init(ks, password);
      keyManagers = kmf.getKeyManagers();
      // follow the security tip in java doc
      if (password != null) {
        java.util.Arrays.fill(password, ' ');
      }
    }
    return keyManagers;
  }

  private boolean isEmpty(String string) {
    return (string == null || string.trim().equals(""));
  }
  
  /**
   * Perform initialization using defaults in {@link 
   * com.gemstone.gemfire.distributed.internal.DistributionConfig}.  If any
   * values are specified in System properties, those values will be used to 
   * override the defaults.
   * <p>
   * Caller must synchronize on the SocketCreator instance.
   */
  private void initialize() {
    initialize(new DistributionConfigImpl(new Properties()));
  }
  
  /**
   * Initialize this SocketCreator using the DistributionConfig.
   * <p>
   * Caller must synchronize on the SocketCreator instance.
   */
  private void initialize(DistributionConfig config) {
    DistributionConfig conf = config;
    if (conf == null) {
      conf = new DistributionConfigImpl(new Properties());
    }
    
    initialize(conf.getClusterSSLEnabled(),
               conf.getClusterSSLRequireAuthentication(),
               readArray(conf.getClusterSSLProtocols()),
               readArray(conf.getClusterSSLCiphers()),
               conf.getClusterSSLProperties());
  }
  
  // -------------------------------------------------------------------------
  //   Public methods
  // -------------------------------------------------------------------------
  
  /** Returns true if this SocketCreator is configured to use SSL. */
  public boolean useSSL() {
    return this.useSSL;
  }
  
  /** 
   * Return a ServerSocket possibly configured for SSL.
   *  SSL configuration is left up to JSSE properties in java.security file.
   */
  public ServerSocket createServerSocket( int nport, int backlog ) throws IOException {
    return createServerSocket( nport, backlog, null );
  }
  
  public ServerSocket createServerSocket(int nport, int backlog,
      InetAddress bindAddr, List<GatewayTransportFilter> transportFilters,
      int socketBufferSize)
      throws IOException {
    if (transportFilters.isEmpty()) {
      return createServerSocket(nport, backlog, bindAddr, socketBufferSize);
    }
    else {
      printConfig();
      ServerSocket result = new TransportFilterServerSocket(transportFilters);
      result.setReuseAddress(true);
      // Set the receive buffer size before binding the socket so
      // that large buffers will be allocated on accepted sockets (see
      // java.net.ServerSocket.setReceiverBufferSize javadocs)
      result.setReceiveBufferSize(socketBufferSize);
      try {
        result.bind(new InetSocketAddress(bindAddr, nport), backlog);
      }
      catch (BindException e) {
        BindException throwMe = new BindException(
            LocalizedStrings.SocketCreator_FAILED_TO_CREATE_SERVER_SOCKET_ON_0_1
                .toLocalizedString(new Object[] { bindAddr,
                    Integer.valueOf(nport) }));
        throwMe.initCause(e);
        throw throwMe;
      }
      return result;
    }
  }
  
  /** 
   * Return a ServerSocket possibly configured for SSL.
   *  SSL configuration is left up to JSSE properties in java.security file.
   */
  public ServerSocket createServerSocket( int nport, int backlog, InetAddress bindAddr ) throws IOException {
    return createServerSocket( nport, backlog, bindAddr, -1 );
  }

  public ServerSocket createServerSocket(int nport, int backlog,
      InetAddress bindAddr, int socketBufferSize)
      throws IOException {
    //       rw.readLock().lockInterruptibly();
//       try {
        printConfig();
        if ( this.useSSL ) {
          if (this.sslContext == null) {
            throw new GemFireConfigException("SSL not configured correctly, Please look at previous error");
          }
          ServerSocketFactory ssf = this.sslContext.getServerSocketFactory();
          SSLServerSocket serverSocket = (SSLServerSocket)ssf.createServerSocket();
          serverSocket.setReuseAddress(true);
          // If necessary, set the receive buffer size before binding the socket so
          // that large buffers will be allocated on accepted sockets (see
          // java.net.ServerSocket.setReceiverBufferSize javadocs)
          if (socketBufferSize != -1) {
            serverSocket.setReceiveBufferSize(socketBufferSize);
          }
          serverSocket.bind(new InetSocketAddress(bindAddr, nport), backlog);
          finishServerSocket( serverSocket );
          return serverSocket;
        } 
        else {
          //log.info("Opening server socket on " + nport, new Exception("SocketCreation"));
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
          }
          catch (BindException e) {
            BindException throwMe = new BindException(LocalizedStrings.SocketCreator_FAILED_TO_CREATE_SERVER_SOCKET_ON_0_1.toLocalizedString(new Object[] {bindAddr, Integer.valueOf(nport)}));
            throwMe.initCause(e);
            throw throwMe;
          }
          return result;
        }
//       }
//       finally {
//         rw.readLock().unlock();
//       }
  }

  /**
   * Creates or bind server socket to a random port selected
   * from tcp-port-range which is same as membership-port-range.
   * @param ba
   * @param backlog
   * @param isBindAddress
   * @param tcpBufferSize
   * @return Returns the new server socket.
   * @throws IOException
   */
  public ServerSocket createServerSocketUsingPortRange(InetAddress ba, int backlog,
      boolean isBindAddress, boolean useNIO, int tcpBufferSize, int[] tcpPortRange)
      throws IOException {
    
    ServerSocket socket = null;
    int localPort = 0;
    int startingPort = 0;
    
    // Get a random port from range.
    Random rand = new SecureRandom();
    int portLimit = tcpPortRange[1];
    int randPort = tcpPortRange[0] + rand.nextInt(tcpPortRange[1] - tcpPortRange[0] + 1);

    startingPort = randPort;
    localPort = startingPort;

    while (true) {
      if (localPort > portLimit) {
        if (startingPort != 0) {
          localPort = tcpPortRange[0];
          portLimit = startingPort - 1;
          startingPort = 0;
        } else {
          throw new SystemConnectException(
              LocalizedStrings.TCPConduit_UNABLE_TO_FIND_FREE_PORT.toLocalizedString());
        }
      }
      try {
        if (useNIO) {
          ServerSocketChannel channl = ServerSocketChannel.open();
          socket = channl.socket();

          InetSocketAddress addr = new InetSocketAddress(isBindAddress ? ba : null, localPort);
          socket.bind(addr, backlog);
        } else {
          socket = SocketCreator.getDefaultInstance().createServerSocket(localPort, backlog, isBindAddress? ba : null, tcpBufferSize);
        }
        break;
      } catch (java.net.SocketException ex) {
        if (useNIO || SocketCreator.treatAsBindException(ex)) {
          localPort++;
        } else {
          throw ex;
        }
      }
    }
    return socket;
  }

  public static boolean treatAsBindException(SocketException se) {
    if(se instanceof BindException) {
      return true;
    }
    final String msg = se.getMessage();
    return (msg != null && msg.contains("Invalid argument: listen failed"));
  }

  /** Return a client socket. This method is used by client/server clients. */
  public Socket connectForClient( String host, int port, int timeout ) throws IOException {
    return connect( InetAddress.getByName( host ), port, timeout, null, true, -1 );
  }
  
  /** Return a client socket. This method is used by client/server clients. */
  public Socket connectForClient( String host, int port, int timeout, int socketBufferSize ) throws IOException {
    return connect( InetAddress.getByName( host ), port, timeout, null, true, socketBufferSize );
  }

  /** Return a client socket. This method is used by peers. */
  public Socket connectForServer( InetAddress inetadd, int port ) throws IOException {
    return connect(inetadd, port, 0, null, false, -1);
  }
 
  /** Return a client socket. This method is used by peers. */
  public Socket connectForServer( InetAddress inetadd, int port, int socketBufferSize ) throws IOException {
    return connect(inetadd, port, 0, null, false, socketBufferSize);
  }
  
  /**
   * Return a client socket, timing out if unable to connect and timeout > 0 (millis).
   * The parameter <i>timeout</i> is ignored if SSL is being used, as there is no
   * timeout argument in the ssl socket factory
   */
  public Socket connect(InetAddress inetadd, int port, int timeout,
      ConnectionWatcher optionalWatcher, boolean clientSide)
    throws IOException {
    return connect(inetadd, port, timeout, optionalWatcher, clientSide, -1);
  }

  /**
   * Return a client socket, timing out if unable to connect and timeout > 0 (millis).
   * The parameter <i>timeout</i> is ignored if SSL is being used, as there is no
   * timeout argument in the ssl socket factory
   */
  public Socket connect(InetAddress inetadd, int port, int timeout,
      ConnectionWatcher optionalWatcher, boolean clientSide, int socketBufferSize) throws IOException {
    return connect(inetadd, port, timeout, optionalWatcher, clientSide, socketBufferSize, this.useSSL);
  }

  /**
   * Return a client socket, timing out if unable to connect and timeout > 0 (millis).
   * The parameter <i>timeout</i> is ignored if SSL is being used, as there is no
   * timeout argument in the ssl socket factory
   */
  public Socket connect(InetAddress inetadd, int port,
      int timeout, ConnectionWatcher optionalWatcher, boolean clientSide,
      int socketBufferSize, boolean sslConnection) throws IOException {
      Socket socket = null;
      SocketAddress sockaddr = new InetSocketAddress(inetadd, port);
      printConfig();
      try {
        if ( sslConnection ) {
          if (this.sslContext == null) {
            throw new GemFireConfigException("SSL not configured correctly, Please look at previous error");
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
          if (timeout > 0) {
            SocketUtils.connect(socket, sockaddr, timeout);
          }
          else {
            SocketUtils.connect(socket, sockaddr, 0);

          }
          configureClientSSLSocket( socket );
          return socket;
        } 
        else {
          if (clientSide && this.clientSocketFactory != null) {
            socket = this.clientSocketFactory.createSocket( inetadd, port );
          } else {
            socket = new Socket( );

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
          if (timeout > 0) {
            SocketUtils.connect(socket, sockaddr, timeout);
            }
            else {
              SocketUtils.connect(socket, sockaddr, 0);
            }
          }
          return socket;
        }
      }
      finally {
        if (optionalWatcher != null) {
          optionalWatcher.afterConnect(socket);
        }
      }
//       }
//       finally {
//         rw.readLock().unlock();
//       }
  }

  /** Will be a server socket... this one simply registers the listeners. */
  public void configureServerSSLSocket( Socket socket ) throws IOException {
//       rw.readLock().lockInterruptibly();
//       try {
        if (socket instanceof SSLSocket) {
          SSLSocket sslSocket = (SSLSocket)socket;
          try {
            sslSocket.startHandshake();
            SSLSession session = sslSocket.getSession();
            Certificate[] peer = session.getPeerCertificates();
            logger.info(LocalizedMessage.create(LocalizedStrings.SocketCreator_SSL_CONNECTION_FROM_PEER_0, ((X509Certificate)peer[0]).getSubjectDN()));
          }
          catch (SSLPeerUnverifiedException ex) {
            if (this.needClientAuth) {
              logger.fatal(LocalizedMessage.create(LocalizedStrings.SocketCreator_SSL_ERROR_IN_AUTHENTICATING_PEER_0_1, new Object[] { socket.getInetAddress(), Integer.valueOf(socket.getPort())}), ex);
              throw ex;
            }
          }
          catch (SSLException ex) {
            logger.fatal(LocalizedMessage.create(LocalizedStrings.SocketCreator_SSL_ERROR_IN_CONNECTING_TO_PEER_0_1, new Object[] { socket.getInetAddress(), Integer.valueOf(socket.getPort())}), ex);
            throw ex;
          }
        } // ...if
// }
// finally {
// rw.readLock().unlock();
// }
  }
  
  // -------------------------------------------------------------------------
  //   Private implementation methods
  // -------------------------------------------------------------------------
  
  /** Configure the SSLServerSocket based on this SocketCreator's settings. */
  private void finishServerSocket( SSLServerSocket serverSocket ) throws IOException {
    serverSocket.setUseClientMode( false );
    if ( this.needClientAuth ) {  
      //serverSocket.setWantClientAuth( true );
      serverSocket.setNeedClientAuth( true );
    }
    serverSocket.setEnableSessionCreation( true );
    
    // restrict cyphers
    if ( ! "any".equalsIgnoreCase( this.protocols[0] ) ) {
      serverSocket.setEnabledProtocols( this.protocols );
    }
    if ( ! "any".equalsIgnoreCase( this.ciphers[0] ) ) {
      serverSocket.setEnabledCipherSuites( this.ciphers );
    }
  }
  
  /** 
   * When a socket is accepted from a server socket, it should be passed to 
   * this method for SSL configuration.
   */
  private void configureClientSSLSocket( Socket socket ) throws IOException {
    if ( socket instanceof SSLSocket ) {
      SSLSocket sslSocket = (SSLSocket) socket;
      
      sslSocket.setUseClientMode( true );
      sslSocket.setEnableSessionCreation( true );
      
      // restrict cyphers
      if ( this.protocols != null && !"any".equalsIgnoreCase(this.protocols[0]) ) {
        sslSocket.setEnabledProtocols( this.protocols );
      }
      if ( this.ciphers != null && !"any".equalsIgnoreCase(this.ciphers[0]) ) {
        sslSocket.setEnabledCipherSuites( this.ciphers );
      }

      try {
        sslSocket.startHandshake();
        SSLSession session = sslSocket.getSession();
        Certificate[] peer = session.getPeerCertificates();
        logger.info(LocalizedMessage.create(LocalizedStrings.SocketCreator_SSL_CONNECTION_FROM_PEER_0, ((X509Certificate)peer[0]).getSubjectDN()));
      }
      catch (SSLPeerUnverifiedException ex) {
        if (this.needClientAuth) {
          logger.fatal(LocalizedMessage.create(LocalizedStrings.SocketCreator_SSL_ERROR_IN_AUTHENTICATING_PEER), ex);
          throw ex;
        }
      }
      catch (SSLException ex) {
        logger.fatal(LocalizedMessage.create(LocalizedStrings.SocketCreator_SSL_ERROR_IN_CONNECTING_TO_PEER_0_1, new Object[] {socket.getInetAddress(), Integer.valueOf(socket.getPort())}), ex);
        throw ex;
      }
      // catch ( IOException e ) {
      // if ( this.needClientAuth ) {
      // logSevere( log, "SSL Error in authenticating peer.", e );
      // throw e;
      // }
      // else {
      // logWarning( log, "SSL Error in authenticating peer.", e );
      //          }
      //        }
    }
  }
  
  /** Print current configured state to log. */
  private void printConfig() {
    if ( ! configShown && logger.isDebugEnabled()) {
      configShown = true;
      StringBuffer sb = new StringBuffer();
      sb.append( "SSL Configuration: \n" );
      sb.append( "  ssl-enabled = " + this.useSSL ).append( "\n" );
      // add other options here....
      for (String key: System.getProperties().stringPropertyNames()) { // fix for 46822
        if ( key.startsWith( "javax.net.ssl" ) ) {
          sb.append( "  " ).append( key ).append( " = " ).append( System.getProperty( key ) ).append( "\n" );
        }
      }
      logger.debug( sb.toString() );
    }
  }
  
  /** Read an array of values from a string, whitespace separated. */
  private static String[] readArray( String text ) {
    if ( text == null || text.trim().equals( "" ) ) {
      return null;
    }
    
    StringTokenizer st = new StringTokenizer( text );
    Vector v = new Vector( );
    while( st.hasMoreTokens() ) {
      v.add( st.nextToken() );
    }
    return (String[]) v.toArray( new String[ v.size() ] );
  }
  
  
  protected void initializeClientSocketFactory() {
    this.clientSocketFactory = null;
    String className = System.getProperty("gemfire.clientSocketFactory");
    if (className != null) {
      Object o;
      try {
        Class c = ClassPathLoader.getLatest().forName(className);
        o = c.newInstance();
      }
      catch (Exception e) {
        // No cache exists yet, so this can't be logged.
        String s = "An unexpected exception occurred while instantiating a "
            + className + ": " + e;
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
  
//   // -------------------------------------------------------------------------
//   //   dummy ReadWriteLock impl's used to compare performance impact of 
//   //     WriterPreferenceReadWriteLock usage
//   // -------------------------------------------------------------------------
  
//   private class RW implements ReadWriteLock {
//     public Sync readLock() {
//       return new S();
//     }
//     public Sync writeLock() {
//       return new S();
//     }
//   }
//   private class S implements Sync {
//     public void acquire() throws InterruptedException {}
//     public boolean attempt(long msecs) throws InterruptedException { return true; }
//     public void release() {}
//   }
  

  /** returns a set of the non-loopback InetAddresses for this machine */
  public static Set<InetAddress> getMyAddresses() {
    Set<InetAddress> result = new HashSet<InetAddress>();
    Set<InetAddress> locals = new HashSet<InetAddress>();
    Enumeration<NetworkInterface> interfaces;
    try {
      interfaces = NetworkInterface.getNetworkInterfaces();
    } catch (SocketException e) {
      throw new IllegalArgumentException(
          LocalizedStrings.StartupMessage_UNABLE_TO_EXAMINE_NETWORK_INTERFACES
            .toLocalizedString(), e);
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
   * @param addr
   * @return the host name associated with the address or null if lookup isn't possible or there is no host name for this address
   */
  public static String reverseDNS(InetAddress addr) {
    byte[] addrBytes = addr.getAddress();
    // reverse the address suitable for reverse lookup
    String lookup = "";
    for (int index = addrBytes.length - 1; index >= 0; index--) {
      lookup = lookup + (addrBytes[index] & 0xff) + '.';
    }
    lookup += "in-addr.arpa";
//    System.out.println("Looking up: " + lookup);

    try {
      Hashtable env = new Hashtable();
      env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.dns.DnsContextFactory");
      DirContext ctx = new InitialDirContext(env);
      Attributes attrs = ctx.getAttributes(lookup, new String[] {"PTR"});
      for (NamingEnumeration ae = attrs.getAll();ae.hasMoreElements();) {
        Attribute attr = (Attribute)ae.next();
        for (Enumeration vals = attr.getAll();vals.hasMoreElements();) {
          Object elem = vals.nextElement();
          if ("PTR".equals(attr.getID()) && elem != null) {
            return elem.toString();
          }
        }
      }
      ctx.close();
    } catch(Exception e) {
      // ignored
    }
    return null;
  }
  
  /** Returns true if host matches the LOCALHOST. */
  public static boolean isLocalHost(Object host) {
    if (host instanceof InetAddress) {
      if (InetAddressUtil.LOCALHOST.equals(host)) {
        return true;
      }
      else {
        try {
          Enumeration en=NetworkInterface.getNetworkInterfaces();
          while(en.hasMoreElements()) {
            NetworkInterface i=(NetworkInterface)en.nextElement();
            for(Enumeration en2=i.getInetAddresses(); en2.hasMoreElements();) {
              InetAddress addr=(InetAddress)en2.nextElement();
              if (host.equals(addr)) {
                return true;
              }
            }
          }
          return false;
        }
        catch (SocketException e) {
          throw new IllegalArgumentException(LocalizedStrings.InetAddressUtil_UNABLE_TO_QUERY_NETWORK_INTERFACE.toLocalizedString(), e);
        }
      }
    }
    else {
      return isLocalHost(toInetAddress(host.toString()));
    }
  }

  /** 
   * Converts the string host to an instance of InetAddress.  Returns null if
   * the string is empty.  Fails Assertion if the conversion would result in
   * <code>java.lang.UnknownHostException</code>.
   * <p>
   * Any leading slashes on host will be ignored.
   *
   * @param   host  string version the InetAddress
   * @return  the host converted to InetAddress instance
   */
  public static InetAddress toInetAddress(String host) {
    if (host == null || host.length() == 0) {
      return null;
    }
    try {
      if (host.indexOf("/") > -1) {
        return InetAddress.getByName(host.substring(host.indexOf("/") + 1));
      }
      else {
        return InetAddress.getByName(host);
      }
    } catch (java.net.UnknownHostException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }
}

