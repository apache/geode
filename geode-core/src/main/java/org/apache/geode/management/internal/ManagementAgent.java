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

package org.apache.geode.management.internal;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.rmi.AlreadyBoundException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMIServerSocketFactory;
import java.rmi.server.UnicastRemoteObject;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXServiceURL;
import javax.management.remote.rmi.RMIConnectorServer;
import javax.management.remote.rmi.RMIJRMPServerImpl;
import javax.management.remote.rmi.RMIServerImpl;

import com.healthmarketscience.rmiio.exporter.RemoteStreamExporter;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import org.apache.geode.GemFireConfigException;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.internal.HttpService;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.internal.net.SSLConfig;
import org.apache.geode.internal.net.SSLConfigurationFactory;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.security.shiro.JMXShiroAuthenticator;
import org.apache.geode.internal.serialization.filter.FilterConfiguration;
import org.apache.geode.internal.serialization.filter.UnableToSetSerialFilterException;
import org.apache.geode.internal.tcp.TCPConduit;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.ManagementException;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.ManagerMXBean;
import org.apache.geode.management.internal.api.LocatorClusterManagementService;
import org.apache.geode.management.internal.beans.FileUploader;
import org.apache.geode.management.internal.security.AccessControlMBean;
import org.apache.geode.management.internal.security.MBeanServerWrapper;
import org.apache.geode.management.internal.security.ResourceConstants;
import org.apache.geode.management.internal.unsafe.ReadOpFileAccessController;
import org.apache.geode.security.AuthTokenEnabledComponents;

/**
 * Agent implementation that controls the JMX server end points for JMX clients to connect, such as
 * an RMI server.
 * <p>
 * The ManagementAgent could be used in a loner or GemFire client to define and control JMX server
 * end points for the Platform MBeanServer and the GemFire MBeans hosted within it.
 *
 * @since GemFire 7.0
 */
public class ManagementAgent {

  private static final Logger logger = LogService.getLogger();
  public static final String SPRING_PROFILES_ACTIVE = "spring.profiles.active";

  static final String JAVA_RMI_SERVER_HOSTNAME = "java.rmi.server.hostname";

  /**
   * True if running. Protected by synchronizing on this Manager instance. I used synchronization
   * because I think we'll want to hold the same synchronize while configuring, starting, and
   * eventually stopping the RMI server, the hidden management regions (in FederatingManager), etc
   */
  private boolean running = false;
  private Registry registry;

  private JMXConnectorServer jmxConnectorServer;
  private final DistributionConfig config;
  private final SecurityService securityService;
  private final InternalCache cache;
  private RMIClientSocketFactory rmiClientSocketFactory;
  private RMIServerSocketFactory rmiServerSocketFactory;
  private int port;
  private RemoteStreamExporter remoteStreamExporter = null;

  private final FilterConfiguration filterConfiguration;

  /**
   * This system property is set to true when the embedded HTTP server is started so that the
   * embedded pulse webapp can use a local MBeanServer instead of a remote JMX connection.
   */
  private static final String PULSE_EMBEDDED_PROP = "pulse.embedded";
  private static final String PULSE_HOST_PROP = "pulse.host";
  private static final String PULSE_PORT_PROP = "pulse.port";
  private static final String PULSE_USESSL_MANAGER = "pulse.useSSL.manager";
  private static final String PULSE_USESSL_LOCATOR = "pulse.useSSL.locator";

  public ManagementAgent(DistributionConfig config, InternalCache cache,
      FilterConfiguration filterConfiguration) {
    this.config = config;
    this.cache = cache;
    securityService = cache.getSecurityService();
    this.filterConfiguration = filterConfiguration;
  }

  public synchronized boolean isRunning() {
    return running;
  }

  public synchronized void startAgent() {
    try {
      filterConfiguration.configure();
    } catch (UnableToSetSerialFilterException e) {
      throw new RuntimeException(e);
    }
    loadWebApplications();

    if (!running && config.getJmxManagerPort() != 0) {
      try {
        configureAndStart();
      } catch (IOException e) {
        throw new ManagementException(e);
      }
      running = true;
    }
  }

  public synchronized void stopAgent() {
    if (!running) {
      return;
    }

    if (logger.isDebugEnabled()) {
      logger.debug("Stopping jmx manager agent");
    }
    try {
      jmxConnectorServer.stop();
      UnicastRemoteObject.unexportObject(registry, true);
    } catch (Exception e) {
      throw new ManagementException(e);
    }

    running = false;
  }

  private final String GEMFIRE_VERSION = GemFireVersion.getGemFireVersion();
  private final AgentUtil agentUtil = new AgentUtil(GEMFIRE_VERSION);

  private void loadWebApplications() {
    final SystemManagementService managementService = (SystemManagementService) ManagementService
        .getManagementService(cache);

    final ManagerMXBean managerBean = managementService.getManagerMXBean();

    if (config.getHttpServicePort() == 0) {
      setStatusMessage(managerBean,
          "Embedded HTTP server configured not to start (http-service-port=0) or (jmx-manager-http-port=0)");
      return;
    }

    // Find the Management rest WAR file
    final URI adminRestWar = agentUtil.findWarLocation("geode-web");
    if (adminRestWar == null) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Unable to find Geode V1 Management REST API WAR file; the Management REST Interface for Geode will not be accessible.");
      }
    }

    // Find the V2 Cluster Management REST API WAR file
    final URI managementRestWar = agentUtil.findWarLocation("geode-web-management");
    if (managementRestWar == null) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Unable to find Geode V2 Cluster Management REST API WAR file; the new Management API will not be accessible.");
      }
    }

    // Find the Pulse WAR file
    final URI pulseWar = agentUtil.findWarLocation("geode-pulse");

    if (pulseWar == null) {
      final String message =
          "Unable to find Pulse web application WAR file; Pulse for Geode will not be accessible";
      setStatusMessage(managerBean, message);
      if (logger.isDebugEnabled()) {
        logger.debug(message);
      }
    } else {
      String pwFile = config.getJmxManagerPasswordFile();
      if (securityService.isIntegratedSecurity()) {
        String[] authTokenEnabledComponents = config.getSecurityAuthTokenEnabledComponents();
        boolean pulseOauth = Arrays.stream(authTokenEnabledComponents)
            .anyMatch(AuthTokenEnabledComponents::hasPulse);
        if (pulseOauth) {
          System.setProperty(SPRING_PROFILES_ACTIVE, "pulse.authentication.oauth");
        } else {
          System.setProperty(SPRING_PROFILES_ACTIVE, "pulse.authentication.gemfire");
        }
      } else if (isNotBlank(pwFile)) {
        System.setProperty(SPRING_PROFILES_ACTIVE, "pulse.authentication.gemfire");
      }
    }

    try {
      HttpService httpService = cache.getService(HttpService.class);
      if (httpService != null && agentUtil.isAnyWarFileAvailable(adminRestWar, pulseWar)) {

        final String bindAddress = config.getHttpServiceBindAddress();
        final int port = config.getHttpServicePort();

        Map<String, Object> serviceAttributes = new HashMap<>();
        serviceAttributes.put(HttpService.SECURITY_SERVICE_SERVLET_CONTEXT_PARAM,
            securityService);

        // Create LocatorClusterManagementService for the V2 Management REST API
        // Pass null for persistenceService if cluster configuration is disabled
        InternalConfigurationPersistenceService persistenceService = null;
        if (config.getEnableClusterConfiguration()) {
          InternalLocator locator = InternalLocator.getLocator();
          if (locator != null) {
            persistenceService = locator.getConfigurationPersistenceService();
          }
        }
        LocatorClusterManagementService clusterManagementService =
            new LocatorClusterManagementService(cache, persistenceService);
        serviceAttributes.put(HttpService.CLUSTER_MANAGEMENT_SERVICE_CONTEXT_PARAM,
            clusterManagementService);

        // Set auth token enabled parameter for management REST APIs
        String[] authTokenEnabledComponents = config.getSecurityAuthTokenEnabledComponents();
        boolean managementAuthTokenEnabled = Arrays.stream(authTokenEnabledComponents)
            .anyMatch(AuthTokenEnabledComponents::hasManagement);
        serviceAttributes.put(HttpService.AUTH_TOKEN_ENABLED_PARAM, managementAuthTokenEnabled);

        // if jmx manager is running, admin rest should be available, either on locator or server
        if (agentUtil.isAnyWarFileAvailable(adminRestWar)) {
          Path adminRestWarPath = Paths.get(adminRestWar);
          httpService.addWebApplication("/gemfire", adminRestWarPath, serviceAttributes);
          httpService.addWebApplication("/geode-mgmt", adminRestWarPath, serviceAttributes);
        }

        // Deploy V2 Cluster Management API at /management context path
        if (agentUtil.isAnyWarFileAvailable(managementRestWar)) {
          Path managementRestWarPath = Paths.get(managementRestWar);
          httpService.addWebApplication("/management", managementRestWarPath, serviceAttributes);
        }

        // if jmx manager is running, pulse should be available, either on locator or server
        // we need to pass in the sllConfig to pulse because it needs it to make jmx connection
        if (agentUtil.isAnyWarFileAvailable(pulseWar)) {
          System.setProperty(PULSE_EMBEDDED_PROP, "true");
          System.setProperty(PULSE_HOST_PROP, "" + config.getJmxManagerBindAddress());
          System.setProperty(PULSE_PORT_PROP, "" + config.getJmxManagerPort());

          final SocketCreator jmxSocketCreator =
              SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.JMX);
          final SocketCreator locatorSocketCreator = SocketCreatorFactory
              .getSocketCreatorForComponent(SecurableCommunicationChannel.LOCATOR);
          System.setProperty(PULSE_USESSL_MANAGER, jmxSocketCreator.forClient().useSSL() + "");
          System.setProperty(PULSE_USESSL_LOCATOR, locatorSocketCreator.forClient().useSSL() + "");

          serviceAttributes.put(HttpService.GEODE_SSLCONFIG_SERVLET_CONTEXT_PARAM,
              createSslProps());

          httpService.addWebApplication("/pulse", Paths.get(pulseWar), serviceAttributes);

          managerBean.setPulseURL("http://".concat(getHost(bindAddress)).concat(":")
              .concat(String.valueOf(port)).concat("/pulse/"));
        }

        // Start/Restart HTTP server after adding all webapps to ensure proper Jetty 12
        // Configuration lifecycle
        // This is critical for ServletContainerInitializer discovery (e.g.,
        // SpringServletContainerInitializer)
        httpService.restartHttpServer();
      }
    } catch (Throwable e) {
      setStatusMessage(managerBean, "HTTP service failed to start with "
          + e.getClass().getSimpleName() + " '" + e.getMessage() + "'");
      throw new ManagementException("HTTP service failed to start", e);
    }
  }

  private Properties createSslProps() {
    Properties sslProps = new Properties();
    SSLConfig sslConfig =
        SSLConfigurationFactory.getSSLConfigForComponent(config, SecurableCommunicationChannel.WEB);

    if (StringUtils.isNotEmpty(sslConfig.getKeystore())) {
      sslProps.put(SSLConfigurationFactory.JAVAX_KEYSTORE, sslConfig.getKeystore());
    }
    if (StringUtils.isNotEmpty(sslConfig.getKeystorePassword())) {
      sslProps.put(SSLConfigurationFactory.JAVAX_KEYSTORE_PASSWORD,
          sslConfig.getKeystorePassword());
    }
    if (StringUtils.isNotEmpty(sslConfig.getKeystoreType())) {
      sslProps.put(SSLConfigurationFactory.JAVAX_KEYSTORE_TYPE, sslConfig.getKeystoreType());
    }
    if (StringUtils.isNotEmpty(sslConfig.getTruststore())) {
      sslProps.put(SSLConfigurationFactory.JAVAX_TRUSTSTORE, sslConfig.getTruststore());
    }
    if (StringUtils.isNotEmpty(sslConfig.getTruststorePassword())) {
      sslProps.put(SSLConfigurationFactory.JAVAX_TRUSTSTORE_PASSWORD,
          sslConfig.getTruststorePassword());
    }
    if (StringUtils.isNotEmpty(sslConfig.getTruststoreType())) {
      sslProps.put(SSLConfigurationFactory.JAVAX_TRUSTSTORE_TYPE, sslConfig.getTruststoreType());
    }
    if (!sslConfig.isAnyCiphers()) {
      sslProps.put("javax.rmi.ssl.client.enabledCipherSuites", sslConfig.getCiphers());
    }
    if (!SSLConfig.isAnyProtocols(sslConfig.getClientProtocols())) {
      sslProps.put("javax.rmi.ssl.client.enabledProtocols", sslConfig.getClientProtocols());
    }

    return sslProps;
  }

  private String getHost(final String bindAddress) throws UnknownHostException {
    if (isNotBlank(config.getJmxManagerHostnameForClients())) {
      return config.getJmxManagerHostnameForClients();
    } else if (isNotBlank(bindAddress)) {
      return InetAddress.getByName(bindAddress).getHostAddress();
    } else {
      return LocalHostUtil.getLocalHost().getHostAddress();
    }
  }

  private void setStatusMessage(ManagerMXBean mBean, String message) {
    mBean.setPulseURL("");
    mBean.setStatusMessage(message);
  }

  /**
   * http://docs.oracle.com/javase/6/docs/technotes/guides/management/agent.html #gdfvq
   * https://blogs.oracle.com/jmxetc/entry/java_5_premain_rmi_connectors
   * https://blogs.oracle.com/jmxetc/entry/building_a_remotely_stoppable_connector
   * https://blogs.oracle.com/jmxetc/entry/jmx_connecting_through_firewalls_using
   * https://blogs.oracle.com/jmxetc/entry/java_5_premain_rmi_connectors
   */
  private void configureAndStart() throws IOException {
    // get the port for RMI Registry and RMI Connector Server
    port = config.getJmxManagerPort();
    final String hostname;
    final InetAddress bindAddress;
    if (StringUtils.isBlank(config.getJmxManagerBindAddress())) {
      hostname = LocalHostUtil.getLocalHostName();
      bindAddress = null;
    } else {
      hostname = config.getJmxManagerBindAddress();
      bindAddress = InetAddress.getByName(hostname);
    }

    final SocketCreator socketCreator =
        SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.JMX);

    final boolean ssl = socketCreator.forClient().useSSL();

    setRmiServerHostname(
        getRmiServerHostname(config.getJmxManagerHostnameForClients(), hostname, ssl));

    if (logger.isDebugEnabled()) {
      logger.debug("Starting jmx manager agent on port {}{}", port,
          (bindAddress != null ? (" bound to " + bindAddress) : "") + (ssl ? " using SSL" : ""));
    }
    rmiClientSocketFactory = ssl ? new ContextAwareSSLRMIClientSocketFactory() : null;
    rmiServerSocketFactory = new GemFireRMIServerSocketFactory(socketCreator, bindAddress);

    // Following is done to prevent rmi causing stop the world gcs
    System.setProperty("sun.rmi.dgc.server.gcInterval", Long.toString(Long.MAX_VALUE - 1));

    // Create the RMI Registry using the SSL socket factories above.
    // In order to use a single port, we must use these factories
    // everywhere, or nowhere. Since we want to use them in the JMX
    // RMI Connector server, we must also use them in the RMI Registry.
    // Otherwise, we wouldn't be able to use a single port.

    // Start an RMI registry on port <port>.
    registry = LocateRegistry.createRegistry(port, rmiClientSocketFactory, rmiServerSocketFactory);

    // Retrieve the PlatformMBeanServer.
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

    // Environment map
    final HashMap<String, Object> env = new HashMap<>();

    // this makes sure the credentials passed to make the connection has to be in the form of String
    // or String[]. Other form of credentials will not get de-serialized
    env.put("jmx.remote.rmi.server.credential.types", new String[] {
        String[].class.getName(),
        String.class.getName()});

    // Manually creates and binds a JMX RMI Connector Server stub with the
    // registry created above: the port we pass here is the port that can
    // be specified in "service:jmx:rmi://"+hostname+":"+port - where the
    // RMI server stub and connection objects will be exported.
    // Here we choose to use the same port as was specified for the
    // RMI Registry. We can do so because we're using \*the same\* client
    // and server socket factories, for the registry itself \*and\* for this
    // object.
    final RMIServerImpl stub =
        new RMIJRMPServerImpl(port, rmiClientSocketFactory, rmiServerSocketFactory, env);

    // Create an RMI connector server.
    //
    // As specified in the JMXServiceURL the RMIServer stub will be
    // registered in the RMI registry running in the local host on
    // port <port> with the name "jmxrmi". This is the same name the
    // out-of-the-box management agent uses to register the RMIServer
    // stub too.
    //
    // The port specified in "service:jmx:rmi://"+hostname+":"+port
    // is the second port, where RMI connection objects will be exported.
    // Here we use the same port as that we choose for the RMI registry.
    // The port for the RMI registry is specified in the second part
    // of the URL, in "rmi://"+hostname+":"+port
    //
    // We construct a JMXServiceURL corresponding to what we have done
    // for our stub...
    final JMXServiceURL url = new JMXServiceURL("service:jmx:rmi://" + hostname + ":" + port
        + "/jndi/rmi://" + hostname + ":" + port + "/jmxrmi");

    // Create an RMI connector server with the JMXServiceURL
    //
    // JDK 1.5 cannot use JMXConnectorServerFactory because of
    // http://bugs.sun.com/view_bug.do?bug_id=5107423
    // but we're using JDK 1.6
    jmxConnectorServer =
        new RMIConnectorServer(new JMXServiceURL("rmi", hostname, port), env, stub, mbs) {
          @Override
          public JMXServiceURL getAddress() {
            return url;
          }

          @Override
          public synchronized void start() throws IOException {
            super.start();
            try {
              registry.bind("jmxrmi", stub.toStub());
            } catch (AlreadyBoundException x) {
              throw new IOException(x.getMessage(), x);
            }
          }
        };

    setMBeanServerForwarder(mbs, env);
    registerAccessControlMBean();
    registerFileUploaderMBean();

    jmxConnectorServer.start();
    if (logger.isDebugEnabled()) {
      logger.debug("Finished starting jmx manager agent.");
    }
  }

  /**
   * Sets the {@code java.rmi.server.hostname} System Property.
   *
   * @param rmiHostname to set property to if not blank
   */
  static void setRmiServerHostname(final @Nullable String rmiHostname) {
    if (isNotBlank(rmiHostname)) {
      if (logger.isDebugEnabled()) {
        logger.debug("Setting RMI hostname to : {}", rmiHostname);
      }
      System.setProperty(JAVA_RMI_SERVER_HOSTNAME, rmiHostname);
    }
  }

  /**
   * Gets the hostname that should be set on the RMI server for based on the given values.
   *
   * @param hostnameForClients the hostname clients will expect to access.
   * @param hostnameForServer the hostname that this server is bound to.
   * @param sslEnabled is SSL enabled.
   * @return {@code hostnameForClients} if not blank, otherwise {@code hostnameForServer} if not
   *         blank and {@code sslEnabled}, otherwise {@code null}.
   */
  static @Nullable String getRmiServerHostname(final @Nullable String hostnameForClients,
      final @Nullable String hostnameForServer, final boolean sslEnabled) {
    if (isNotBlank(hostnameForClients)) {
      return hostnameForClients;
    } else if (sslEnabled && isNotBlank(hostnameForServer)) {
      return hostnameForServer;
    }
    return null;
  }

  @VisibleForTesting
  void setMBeanServerForwarder(MBeanServer mbs, HashMap<String, Object> env)
      throws IOException {
    if (securityService.isIntegratedSecurity()) {
      JMXShiroAuthenticator shiroAuthenticator = new JMXShiroAuthenticator(securityService);
      env.put(JMXConnectorServer.AUTHENTICATOR, shiroAuthenticator);
      jmxConnectorServer.addNotificationListener(shiroAuthenticator, null,
          jmxConnectorServer.getAttributes());
      // always going to assume authorization is needed as well, if no custom AccessControl, then
      // the CustomAuthRealm should take care of that
      MBeanServerWrapper mBeanServerWrapper = new MBeanServerWrapper(securityService);
      jmxConnectorServer.setMBeanServerForwarder(mBeanServerWrapper);
    } else {
      /* Disable the old authenticator mechanism */
      String pwFile = config.getJmxManagerPasswordFile();
      if (pwFile != null && pwFile.length() > 0) {
        env.put("jmx.remote.x.password.file", pwFile);
      }

      String accessFile = config.getJmxManagerAccessFile();
      if (accessFile != null && accessFile.length() > 0) {
        // Rewire the mbs hierarchy to set accessController
        ReadOpFileAccessController controller = new ReadOpFileAccessController(accessFile);
        jmxConnectorServer.setMBeanServerForwarder(controller);
      } else {
        // if no access control, do not allow mbean creation to prevent Mlet attack
        jmxConnectorServer.setMBeanServerForwarder(new BlockMBeanCreationController());
      }
    }
  }

  private void registerAccessControlMBean() {
    try {
      AccessControlMBean acc = new AccessControlMBean(securityService);
      ObjectName accessControlMBeanON = new ObjectName(ResourceConstants.OBJECT_NAME_ACCESSCONTROL);
      MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();

      Set<ObjectName> names = platformMBeanServer.queryNames(accessControlMBeanON, null);
      if (names.isEmpty()) {
        try {
          platformMBeanServer.registerMBean(acc, accessControlMBeanON);
          logger.info("Registered AccessControlMBean on " + accessControlMBeanON);
        } catch (InstanceAlreadyExistsException | MBeanRegistrationException
            | NotCompliantMBeanException e) {
          throw new GemFireConfigException(
              "Error while configuring access control for jmx resource", e);
        }
      }
    } catch (MalformedObjectNameException e) {
      throw new GemFireConfigException("Error while configuring access control for jmx resource",
          e);
    }
  }

  private void registerFileUploaderMBean() {
    try {
      ObjectName mbeanON = new ObjectName(ManagementConstants.OBJECTNAME__FILEUPLOADER_MBEAN);
      MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();

      Set<ObjectName> names = platformMBeanServer.queryNames(mbeanON, null);
      if (names.isEmpty()) {
        platformMBeanServer.registerMBean(new FileUploader(getRemoteStreamExporter()), mbeanON);
        logger.info("Registered FileUploaderMBean on " + mbeanON);
      }
    } catch (InstanceAlreadyExistsException | MBeanRegistrationException
        | NotCompliantMBeanException | MalformedObjectNameException e) {
      throw new GemFireConfigException("Error while configuring FileUploader MBean", e);
    }
  }

  public JMXConnectorServer getJmxConnectorServer() {
    return jmxConnectorServer;
  }

  @VisibleForTesting
  void setJmxConnectorServer(JMXConnectorServer jmxConnectorServer) {
    this.jmxConnectorServer = jmxConnectorServer;
  }

  public synchronized RemoteStreamExporter getRemoteStreamExporter() {
    if (remoteStreamExporter == null) {
      remoteStreamExporter =
          new GeodeRemoteStreamExporter(port, rmiClientSocketFactory, rmiServerSocketFactory);
    }
    return remoteStreamExporter;
  }

  private static class GemFireRMIServerSocketFactory implements RMIServerSocketFactory {

    private final SocketCreator sc;
    private final InetAddress bindAddress;

    public GemFireRMIServerSocketFactory(SocketCreator sc, InetAddress bindAddress) {
      this.sc = sc;
      this.bindAddress = bindAddress;
    }

    @Override
    public ServerSocket createServerSocket(int port) throws IOException {
      return sc.forCluster().createServerSocket(port, TCPConduit.getBackLog(), bindAddress);
    }
  }
}
