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
package org.apache.geode.management.internal;

import java.io.IOException;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.rmi.AlreadyBoundException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMIServerSocketFactory;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
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
import javax.rmi.ssl.SslRMIClientSocketFactory;

import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;

import org.apache.geode.GemFireConfigException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.lang.StringUtils;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.net.SSLConfigurationFactory;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.internal.security.shiro.JMXShiroAuthenticator;
import org.apache.geode.internal.tcp.TCPConduit;
import org.apache.geode.management.ManagementException;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.ManagerMXBean;
import org.apache.geode.management.internal.security.AccessControlMBean;
import org.apache.geode.management.internal.security.MBeanServerWrapper;
import org.apache.geode.management.internal.security.ResourceConstants;
import org.apache.geode.management.internal.unsafe.ReadOpFileAccessController;

/**
 * Agent implementation that controls the JMX server end points for JMX clients
 * to connect, such as an RMI server.
 * <p>
 * The ManagementAgent could be used in a loner or GemFire client to define and
 * control JMX server end points for the Platform MBeanServer and the GemFire
 * MBeans hosted within it.
 * @since GemFire 7.0
 */
public class ManagementAgent {

  private static final Logger logger = LogService.getLogger();

  /**
   * True if running. Protected by synchronizing on this Manager instance. I
   * used synchronization because I think we'll want to hold the same
   * synchronize while configuring, starting, and eventually stopping the RMI
   * server, the hidden management regions (in FederatingManager), etc
   */
  private boolean running = false;
  private Registry registry;
  private JMXConnectorServer jmxConnectorServer;
  private JMXShiroAuthenticator shiroAuthenticator;
  private final DistributionConfig config;
  private SecurityService securityService = SecurityService.getSecurityService();
  private boolean isHttpServiceRunning = false;

  /**
   * This system property is set to true when the embedded HTTP server is
   * started so that the embedded pulse webapp can use a local MBeanServer
   * instead of a remote JMX connection.
   */
  private static final String PULSE_EMBEDDED_PROP = "pulse.embedded";

  public ManagementAgent(DistributionConfig config) {
    this.config = config;
  }

  public synchronized boolean isRunning() {
    return this.running;
  }

  public synchronized boolean isHttpServiceRunning() {
    return isHttpServiceRunning;
  }

  public synchronized void setHttpServiceRunning(boolean isHttpServiceRunning) {
    this.isHttpServiceRunning = isHttpServiceRunning;
  }

  private boolean isAPIRestServiceRunning(GemFireCacheImpl cache) {
    return (cache != null && cache.getRestAgent() != null && cache.getRestAgent().isRunning());
  }

  private boolean isServerNode(GemFireCacheImpl cache) {
    return (cache.getDistributedSystem().getDistributedMember().getVmKind() != DistributionManager.LOCATOR_DM_TYPE && cache.getDistributedSystem()
                                                                                                                           .getDistributedMember()
                                                                                                                           .getVmKind() != DistributionManager.ADMIN_ONLY_DM_TYPE && !cache
      .isClient());
  }

  public synchronized void startAgent(GemFireCacheImpl cache) {
    // Do not start Management REST service if developer REST service is already
    // started.

    if (!isAPIRestServiceRunning(cache)) {
      startHttpService(isServerNode(cache));
    } else {
      if (logger.isDebugEnabled()) {
        logger.debug("Developer REST APIs webapp is already running, Not Starting M&M REST and pulse!");
      }
    }

    if (!this.running && this.config.getJmxManagerPort() != 0) {
      try {
        configureAndStart();
      } catch (IOException e) {
        throw new ManagementException(e);
      }
      this.running = true;
    }
  }

  public synchronized void stopAgent() {
    stopHttpService();

    if (!this.running) {
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

    this.running = false;
  }

  private Server httpServer;
  private final String GEMFIRE_VERSION = GemFireVersion.getGemFireVersion();
  private AgentUtil agentUtil = new AgentUtil(GEMFIRE_VERSION);

  private void startHttpService(boolean isServer) {
    final SystemManagementService managementService = (SystemManagementService) ManagementService.getManagementService(CacheFactory.getAnyInstance());

    final ManagerMXBean managerBean = managementService.getManagerMXBean();

    if (this.config.getHttpServicePort() != 0) {
      if (logger.isDebugEnabled()) {
        logger.debug("Attempting to start HTTP service on port ({}) at bind-address ({})...", this.config.getHttpServicePort(), this.config.getHttpServiceBindAddress());
      }

      // Find the Management WAR file
      final String gemfireWar = agentUtil.findWarLocation("geode-web");
      if (gemfireWar == null) {
        if (logger.isDebugEnabled()) {
          logger.debug("Unable to find GemFire Management REST API WAR file; the Management REST Interface for GemFire will not be accessible.");
        }
      }

      // Find the Pulse WAR file
      final String pulseWar = agentUtil.findWarLocation("geode-pulse");

      if (pulseWar == null) {
        final String message = "Unable to find Pulse web application WAR file; Pulse for GemFire will not be accessible";
        setStatusMessage(managerBean, message);
        if (logger.isDebugEnabled()) {
          logger.debug(message);
        }
      } else if (securityService.isIntegratedSecurity()) {
        System.setProperty("spring.profiles.active", "pulse.authentication.gemfire");
      }

      // Find developer REST WAR file
      final String gemfireAPIWar = agentUtil.findWarLocation("geode-web-api");
      if (gemfireAPIWar == null) {
        final String message = "Unable to find GemFire Developer REST API WAR file; the Developer REST Interface for GemFire will not be accessible.";
        setStatusMessage(managerBean, message);
        if (logger.isDebugEnabled()) {
          logger.debug(message);
        }
      }

      try {
        if (agentUtil.isWebApplicationAvailable(gemfireWar, pulseWar, gemfireAPIWar)) {

          final String bindAddress = this.config.getHttpServiceBindAddress();
          final int port = this.config.getHttpServicePort();

          boolean isRestWebAppAdded = false;

          this.httpServer = JettyHelper.initJetty(bindAddress, port, SSLConfigurationFactory.getSSLConfigForComponent(SecurableCommunicationChannel.WEB));

          if (agentUtil.isWebApplicationAvailable(gemfireWar)) {
            this.httpServer = JettyHelper
                .addWebApplication(this.httpServer, "/gemfire", gemfireWar);
            this.httpServer = JettyHelper
                .addWebApplication(this.httpServer, "/geode-mgmt", gemfireWar);
          }

          if (agentUtil.isWebApplicationAvailable(pulseWar)) {
            this.httpServer = JettyHelper.addWebApplication(this.httpServer, "/pulse", pulseWar);
          }

          if (isServer && this.config.getStartDevRestApi()) {
            if (agentUtil.isWebApplicationAvailable(gemfireAPIWar)) {
              this.httpServer = JettyHelper.addWebApplication(this.httpServer, "/geode",
                  gemfireAPIWar);
              this.httpServer = JettyHelper.addWebApplication(this.httpServer, "/gemfire-api",
                  gemfireAPIWar);
              isRestWebAppAdded = true;
            }
          } else {
            final String message = "Developer REST API web application will not start when start-dev-rest-api is not set and node is not server";
            setStatusMessage(managerBean, message);
            if (logger.isDebugEnabled()) {
              logger.debug(message);
            }
          }

          if (logger.isDebugEnabled()) {
            logger.debug("Starting HTTP embedded server on port ({}) at bind-address ({})...", ((ServerConnector) this.httpServer.getConnectors()[0]).getPort(), bindAddress);
          }

          System.setProperty(PULSE_EMBEDDED_PROP, "true");

          this.httpServer = JettyHelper.startJetty(this.httpServer);

          // now, that Tomcat has been started, we can set the URL used by web
          // clients to connect to Pulse
          if (agentUtil.isWebApplicationAvailable(pulseWar)) {
            managerBean.setPulseURL("http://".concat(getHost(bindAddress)).concat(":").concat(String.valueOf(port)).concat("/pulse/"));
          }

          // set cache property for developer REST service running
          if (isRestWebAppAdded) {
            GemFireCacheImpl cache = (GemFireCacheImpl) CacheFactory.getAnyInstance();
            cache.setRESTServiceRunning(true);

            // create region to hold query information (queryId, queryString).
            // Added for the developer REST APIs
            RestAgent.createParameterizedQueryRegion();
          }

          // set true for HTTP service running
          setHttpServiceRunning(true);
        }
      } catch (Exception e) {
        stopHttpService();// Jetty needs to be stopped even if it has failed to
        // start. Some of the threads are left behind even if
        // server.start() fails due to an exception
        setStatusMessage(managerBean, "HTTP service failed to start with " + e.getClass().getSimpleName() + " '" + e.getMessage() + "'");
        throw new ManagementException("HTTP service failed to start", e);
      }
    } else {
      setStatusMessage(managerBean, "Embedded HTTP server configured not to start (http-service-port=0) or (jmx-manager-http-port=0)");
    }
  }

  private String getHost(final String bindAddress) throws UnknownHostException {
    if (!StringUtils.isBlank(this.config.getJmxManagerHostnameForClients())) {
      return this.config.getJmxManagerHostnameForClients();
    } else if (!StringUtils.isBlank(bindAddress)) {
      return InetAddress.getByName(bindAddress).getHostAddress();
    } else {
      return SocketCreator.getLocalHost().getHostAddress();
    }
  }

  private void setStatusMessage(ManagerMXBean mBean, String message) {
    mBean.setPulseURL("");
    mBean.setStatusMessage(message);
  }

  private void stopHttpService() {
    if (this.httpServer != null) {
      if (logger.isDebugEnabled()) {
        logger.debug("Stopping the HTTP service...");
      }
      try {
        this.httpServer.stop();
      } catch (Exception e) {
        logger.warn("Failed to stop the HTTP service because: {}", e.getMessage(), e);
      } finally {
        try {
          this.httpServer.destroy();
        } catch (Exception ignore) {
          logger.error("Failed to properly release resources held by the HTTP service: {}", ignore.getMessage(), ignore);
        } finally {
          this.httpServer = null;
          System.clearProperty("catalina.base");
          System.clearProperty("catalina.home");
        }
      }
    }
  }

  /**
   * http://docs.oracle.com/javase/6/docs/technotes/guides/management/agent.html
   * #gdfvq https://blogs.oracle.com/jmxetc/entry/java_5_premain_rmi_connectors
   * https://blogs.oracle.com/jmxetc/entry/building_a_remotely_stoppable_connector
   * https://blogs.oracle.com/jmxetc/entry/jmx_connecting_through_firewalls_using
   */
  private void configureAndStart() throws IOException {
    // KIRK: I copied this from
    // https://blogs.oracle.com/jmxetc/entry/java_5_premain_rmi_connectors
    // we'll need to change this significantly but it's a starting point

    // get the port for RMI Registry and RMI Connector Server
    final int port = this.config.getJmxManagerPort();
    final String hostname;
    final InetAddress bindAddr;
    if (StringUtils.isBlank(this.config.getJmxManagerBindAddress())) {
      hostname = SocketCreator.getLocalHost().getHostName();
      bindAddr = null;
    } else {
      hostname = this.config.getJmxManagerBindAddress();
      bindAddr = InetAddress.getByName(hostname);
    }

    String jmxManagerHostnameForClients = this.config.getJmxManagerHostnameForClients();
    if (!StringUtils.isBlank(jmxManagerHostnameForClients)) {
      System.setProperty("java.rmi.server.hostname", jmxManagerHostnameForClients);
    }

    final SocketCreator socketCreator = SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.JMX);

    final boolean ssl = socketCreator.useSSL();

    if (logger.isDebugEnabled()) {
      logger.debug("Starting jmx manager agent on port {}{}", port, (bindAddr != null ? (" bound to " + bindAddr) : "") + (ssl ? " using SSL" : ""));
    }
    RMIClientSocketFactory rmiClientSocketFactory = ssl ? new SslRMIClientSocketFactory() : null;// RMISocketFactory.getDefaultSocketFactory();
    // new GemFireRMIClientSocketFactory(sc, getLogger());
    RMIServerSocketFactory rmiServerSocketFactory = new GemFireRMIServerSocketFactory(socketCreator, bindAddr);

    // Following is done to prevent rmi causing stop the world gcs
    System.setProperty("sun.rmi.dgc.server.gcInterval", Long.toString(Long.MAX_VALUE - 1));

    // Create the RMI Registry using the SSL socket factories above.
    // In order to use a single port, we must use these factories
    // everywhere, or nowhere. Since we want to use them in the JMX
    // RMI Connector server, we must also use them in the RMI Registry.
    // Otherwise, we wouldn't be able to use a single port.
    //
    // Start an RMI registry on port <port>.
    registry = LocateRegistry.createRegistry(port, rmiClientSocketFactory, rmiServerSocketFactory);

    // Retrieve the PlatformMBeanServer.
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

    // Environment map. KIRK: why is this declared as HashMap?
    final HashMap<String, Object> env = new HashMap<String, Object>();

    // Manually creates and binds a JMX RMI Connector Server stub with the
    // registry created above: the port we pass here is the port that can
    // be specified in "service:jmx:rmi://"+hostname+":"+port - where the
    // RMI server stub and connection objects will be exported.
    // Here we choose to use the same port as was specified for the
    // RMI Registry. We can do so because we're using \*the same\* client
    // and server socket factories, for the registry itself \*and\* for this
    // object.
    final RMIServerImpl stub = new RMIJRMPServerImpl(port, rmiClientSocketFactory, rmiServerSocketFactory, env);

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
    final JMXServiceURL url = new JMXServiceURL("service:jmx:rmi://" + hostname + ":" + port + "/jndi/rmi://" + hostname + ":" + port + "/jmxrmi");

    // Create an RMI connector server with the JMXServiceURL
    //
    // KIRK: JDK 1.5 cannot use JMXConnectorServerFactory because of
    // http://bugs.sun.com/view_bug.do?bug_id=5107423
    // but we're using JDK 1.6
    jmxConnectorServer = new RMIConnectorServer(new JMXServiceURL("rmi", hostname, port), env, stub, mbs) {
      @Override
      public JMXServiceURL getAddress() {
        return url;
      }

      @Override
      public synchronized void start() throws IOException {
        try {
          registry.bind("jmxrmi", stub);
        } catch (AlreadyBoundException x) {
          final IOException io = new IOException(x.getMessage());
          io.initCause(x);
          throw io;
        }
        super.start();
      }
    };

    if (securityService.isIntegratedSecurity()) {
      shiroAuthenticator = new JMXShiroAuthenticator();
      env.put(JMXConnectorServer.AUTHENTICATOR, shiroAuthenticator);
      jmxConnectorServer.addNotificationListener(shiroAuthenticator, null, jmxConnectorServer.getAttributes());
      // always going to assume authorization is needed as well, if no custom AccessControl, then the CustomAuthRealm
      // should take care of that
      MBeanServerWrapper mBeanServerWrapper = new MBeanServerWrapper();
      jmxConnectorServer.setMBeanServerForwarder(mBeanServerWrapper);
      registerAccessControlMBean();
    } else {
      /* Disable the old authenticator mechanism */
      String pwFile = this.config.getJmxManagerPasswordFile();
      if (pwFile != null && pwFile.length() > 0) {
        env.put("jmx.remote.x.password.file", pwFile);
      }

      String accessFile = this.config.getJmxManagerAccessFile();
      if (accessFile != null && accessFile.length() > 0) {
        // Lets not use default connector based authorization
        // env.put("jmx.remote.x.access.file", accessFile);
        // Rewire the mbs hierarchy to set accessController
        ReadOpFileAccessController controller = new ReadOpFileAccessController(accessFile);
        controller.setMBeanServer(mbs);
        mbs = controller;
      }
    }

    jmxConnectorServer.start();
    if (logger.isDebugEnabled()) {
      logger.debug("Finished starting jmx manager agent.");
    }
  }

  private void registerAccessControlMBean() {
    try {
      AccessControlMBean acc = new AccessControlMBean();
      ObjectName accessControlMBeanON = new ObjectName(ResourceConstants.OBJECT_NAME_ACCESSCONTROL);
      MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();

      Set<ObjectName> names = platformMBeanServer.queryNames(accessControlMBeanON, null);
      if (names.isEmpty()) {
        try {
          platformMBeanServer.registerMBean(acc, accessControlMBeanON);
          logger.info("Registered AccessContorlMBean on " + accessControlMBeanON);
        } catch (InstanceAlreadyExistsException e) {
          throw new GemFireConfigException("Error while configuring accesscontrol for jmx resource", e);
        } catch (MBeanRegistrationException e) {
          throw new GemFireConfigException("Error while configuring accesscontrol for jmx resource", e);
        } catch (NotCompliantMBeanException e) {
          throw new GemFireConfigException("Error while configuring accesscontrol for jmx resource", e);
        }
      }
    } catch (MalformedObjectNameException e) {
      throw new GemFireConfigException("Error while configuring accesscontrol for jmx resource", e);
    }
  }

  private static class GemFireRMIClientSocketFactory implements RMIClientSocketFactory, Serializable {

    private static final long serialVersionUID = -7604285019188827617L;

    private/* final hack to prevent serialization */ transient SocketCreator sc;

    public GemFireRMIClientSocketFactory(SocketCreator sc) {
      this.sc = sc;
    }

    @Override
    public Socket createSocket(String host, int port) throws IOException {
      return this.sc.connectForClient(host, port, 0/* no timeout */);
    }
  }

  ;

  private static class GemFireRMIServerSocketFactory implements RMIServerSocketFactory, Serializable {

    private static final long serialVersionUID = -811909050641332716L;
    private/* final hack to prevent serialization */ transient SocketCreator sc;
    private final InetAddress bindAddr;

    public GemFireRMIServerSocketFactory(SocketCreator sc, InetAddress bindAddr) {
      this.sc = sc;
      this.bindAddr = bindAddr;
    }

    @Override
    public ServerSocket createServerSocket(int port) throws IOException {
      return this.sc.createServerSocket(port, TCPConduit.getBackLog(), this.bindAddr);
    }
  }

  ;
}
