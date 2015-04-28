/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal;

import java.io.File;
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

import javax.management.MBeanServer;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXServiceURL;
import javax.management.remote.rmi.RMIConnectorServer;
import javax.management.remote.rmi.RMIJRMPServerImpl;
import javax.management.remote.rmi.RMIServerImpl;
import javax.rmi.ssl.SslRMIClientSocketFactory;

import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;

import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.internal.GemFireVersion;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.lang.StringUtils;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.tcp.TCPConduit;
import com.gemstone.gemfire.management.ManagementException;
import com.gemstone.gemfire.management.ManagementService;
import com.gemstone.gemfire.management.ManagerMXBean;
import com.gemstone.gemfire.management.internal.security.ManagementInterceptor;
import com.gemstone.gemfire.management.internal.unsafe.ReadOpFileAccessController;

/**
 * Agent implementation that controls the JMX server end points for JMX 
 * clients to connect, such as an RMI server.
 * 
 * The ManagementAgent could be used in a loner or GemFire client to define and
 * control JMX server end points for the Platform MBeanServer and the GemFire
 * MBeans hosted within it. 
 *
 * @author VMware, Inc.
 * @since 7.0
 */
public class ManagementAgent  {
  private static final Logger logger = LogService.getLogger();
  
  /**
   * True if running. Protected by synchronizing on this Manager instance. I
   * used synchronization because I think we'll want to hold the same
   * synchronize while configuring, starting, and eventually stopping the
   * RMI server, the hidden management regions (in FederatingManager), etc
   */
  private boolean running = false;
  private Registry registry;
  private JMXConnectorServer cs;
  private final DistributionConfig config;
  private boolean isHttpServiceRunning = false;
  private ManagementInterceptor securityInterceptor;

  /**
   * This system property is set to true when the embedded HTTP server is started so that the embedded pulse webapp
   * can use a local MBeanServer instead of a remote JMX connection.
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
  
  private boolean isAPIRestServiceRunning(GemFireCacheImpl cache){
    return (cache != null && cache.getRestAgent() != null && cache.getRestAgent().isRunning());
  }
  
  private boolean isServerNode(GemFireCacheImpl cache){
    return (cache.getDistributedSystem().getDistributedMember().getVmKind() != DistributionManager.LOCATOR_DM_TYPE
         && cache.getDistributedSystem().getDistributedMember().getVmKind() != DistributionManager.ADMIN_ONLY_DM_TYPE
         && !cache.isClient());
  }
  
  public synchronized void startAgent(GemFireCacheImpl cache){
    //Do not start Management REST service if developer REST service is already started.
    
    if(!isAPIRestServiceRunning(cache)) {
      startHttpService(isServerNode(cache));
    }else {
      if (logger.isDebugEnabled()) {
        logger.debug("Developer REST APIs webapp is already running, Not Starting M&M REST and pulse!");
      }
    }
    
    if (!this.running && this.config.getJmxManagerPort() != 0) {
      try {
        configureAndStart();
      }
      catch (IOException e) {
        throw new ManagementException(e);
      }
      this.running = true;
    }
  }
  
  public synchronized void stopAgent(){
    stopHttpService();
    
    if (!this.running) return;
    
    if (logger.isDebugEnabled()) {
      logger.debug("Stopping jmx manager agent");
    }
    try {
      cs.stop();
      UnicastRemoteObject.unexportObject(registry, true);
    } catch (IOException e) {
      throw new ManagementException(e);
    }
    
    this.running = false;
  }
  
  private Server httpServer;
  private final String GEMFIRE_VERSION = GemFireVersion.getGemFireVersion();
  
  private void startHttpService(boolean isServer) {
    final SystemManagementService managementService = (SystemManagementService) ManagementService.getManagementService(
      CacheFactory.getAnyInstance());

    final ManagerMXBean managerBean = managementService.getManagerMXBean();
    
    if (this.config.getHttpServicePort() != 0) {
      if (logger.isDebugEnabled()) {
        logger.debug("Attempting to start HTTP service on port ({}) at bind-address ({})...",
            this.config.getHttpServicePort(), this.config.getHttpServiceBindAddress());
      }

      // GEMFIRE environment variable
      final String gemfireHome = System.getenv("GEMFIRE");

      // Check for empty variable. if empty, then log message and exit HTTP server startup
      if (StringUtils.isBlank(gemfireHome)) {
        final String message = "GEMFIRE environment variable not set; HTTP service will not start.";
        setStatusMessage(managerBean, message);
        if (logger.isDebugEnabled()) {
          logger.debug(message);
        }
        return;
      }

      // Find the Management WAR file
      final String gemfireWar = getGemFireWarLocation(gemfireHome);

      if (gemfireWar == null) {
        if (logger.isDebugEnabled()) {
          logger.debug("Unable to find GemFire REST API WAR file; the REST API to GemFire will not be exported and accessible.");
        }
      }

      // Find the Pulse WAR file
      final String pulseWar = getPulseWarLocation(gemfireHome);

      if (pulseWar == null) {
        final String message = "Unable to find Pulse web application WAR file; Pulse will not start in embeded mode";
        setStatusMessage(managerBean, message);
        if (logger.isDebugEnabled()) {
          logger.debug(message);
        }
      }
      
      //Find developer REST WAR file
      final String gemfireAPIWar =  getGemFireAPIWarLocation(gemfireHome);
      if (gemfireAPIWar == null) {
        final String message = "Unable to find developer REST web application WAR file; developer REST will not start in embeded mode";
        setStatusMessage(managerBean, message);
        if (logger.isDebugEnabled()) {
          logger.debug(message);
        }
      }
      
      try {
        if (isWebApplicationAvailable(gemfireWar, pulseWar, gemfireAPIWar)) {
          
          final String bindAddress = this.config.getHttpServiceBindAddress();
          final int port = this.config.getHttpServicePort();

          boolean isRestWebAppAdded = false;
          
          this.httpServer = JettyHelper.initJetty(bindAddress, port, 
              this.config.getHttpServiceSSLEnabled(),
              this.config.getHttpServiceSSLRequireAuthentication(), 
              this.config.getHttpServiceSSLProtocols(),
              this.config.getHttpServiceSSLCiphers(), 
              this.config.getHttpServiceSSLProperties());

          if (isWebApplicationAvailable(gemfireWar)) {
            this.httpServer = JettyHelper.addWebApplication(this.httpServer, "/gemfire", gemfireWar);
          }

          if (isWebApplicationAvailable(pulseWar)) {
            this.httpServer = JettyHelper.addWebApplication(this.httpServer, "/pulse", pulseWar);
          }
         
          if(isServer && this.config.getStartDevRestApi()) {
            if (isWebApplicationAvailable(gemfireAPIWar) ) {
              this.httpServer = JettyHelper.addWebApplication(this.httpServer, "/gemfire-api", gemfireAPIWar);
              isRestWebAppAdded = true;
            }
          }else {
            final String message = "developer REST web application will not start when start-dev-rest-api is not set and node is not server";
            setStatusMessage(managerBean, message);
            if (logger.isDebugEnabled()) {
              logger.debug(message);
            }
          }
          
          if (logger.isDebugEnabled()) {
            logger.debug("Starting HTTP embedded server on port ({}) at bind-address ({})...",
                ((ServerConnector)this.httpServer.getConnectors()[0]).getPort(), bindAddress);
          }

          System.setProperty(PULSE_EMBEDDED_PROP, "true");

          this.httpServer = JettyHelper.startJetty(this.httpServer);

          // now, that Tomcat has been started, we can set the URL used by web clients to connect to Pulse
          if (isWebApplicationAvailable(pulseWar)) {
            managerBean.setPulseURL("http://".concat(getHost(bindAddress)).concat(":").concat(String.valueOf(port))
              .concat("/pulse/"));
          }
          
          //set cache property for developer REST service running
          if(isRestWebAppAdded) {
            GemFireCacheImpl cache = (GemFireCacheImpl)CacheFactory.getAnyInstance();
            cache.setRESTServiceRunning(true);
            
            //create region to hold query information (queryId, queryString). Added for the developer REST APIs
            RestAgent.createParameterizedQueryRegion();
          }
          
          //set true for HTTP service running
          setHttpServiceRunning(true);
        }
      }
      catch (Exception e) {
        stopHttpService();//Jetty needs to be stopped even if it has failed to start. Some of the threads are left behind even if server.start() fails due to an exception
        setStatusMessage(managerBean, "HTTP service failed to start with " + e.getClass().getSimpleName() + " '" + e.getMessage() + "'");
        throw new ManagementException("HTTP service failed to start", e);
      }
    }
    else {
      setStatusMessage(managerBean, "Embedded HTTP server configured not to start (http-service-port=0) or (jmx-manager-http-port=0)");
    }
  }
  
  private String getHost(final String bindAddress) throws UnknownHostException {
    if (!StringUtils.isBlank(this.config.getJmxManagerHostnameForClients())) {
      return this.config.getJmxManagerHostnameForClients();
    }
    else if (!StringUtils.isBlank(bindAddress)) {
      return InetAddress.getByName(bindAddress).getHostAddress();
    }
    else {
      return SocketCreator.getLocalHost().getHostAddress();
    }
  }

  // Use the GEMFIRE environment variable to find the GemFire product tree.
  // First, look in the $GEMFIRE/tools/Management directory
  // Second, look in the $GEMFIRE/lib directory
  // Finally, if we cannot find Management WAR file then return null...
  private String getGemFireWarLocation(final String gemfireHome) {
    assert !StringUtils.isBlank(gemfireHome) : "The GEMFIRE environment variable must be set!";

    if (new File(gemfireHome + "/tools/Extensions/gemfire-web-" + GEMFIRE_VERSION + ".war").isFile()) {
      return gemfireHome + "/tools/Extensions/gemfire-web-" + GEMFIRE_VERSION + ".war";
    }
    else if (new File(gemfireHome + "/lib/gemfire-web-" + GEMFIRE_VERSION + ".war").isFile()) {
      return gemfireHome + "/lib/gemfire-web-" + GEMFIRE_VERSION + ".war";
    }
    else {
      return null;
    }
  }

  // Use the GEMFIRE environment variable to find the GemFire product tree.
  // First, look in the $GEMFIRE/tools/Pulse directory
  // Second, look in the $GEMFIRE/lib directory
  // Finally, if we cannot find the Management WAR file then return null...
  private String getPulseWarLocation(final String gemfireHome) {
    assert !StringUtils.isBlank(gemfireHome) : "The GEMFIRE environment variable must be set!";

    if (new File(gemfireHome + "/tools/Pulse/pulse.war").isFile()) {
      return gemfireHome + "/tools/Pulse/pulse.war";
    }
    else if (new File(gemfireHome + "/lib/pulse.war").isFile()) {
      return gemfireHome + "/lib/pulse.war";
    }
    else {
      return null;
    }
  }

  private String getGemFireAPIWarLocation(final String gemfireHome) {
    assert !StringUtils.isBlank(gemfireHome) : "The GEMFIRE environment variable must be set!";
    if (new File(gemfireHome + "/tools/Extensions/gemfire-api" + GEMFIRE_VERSION + ".war").isFile()) {
      return gemfireHome + "/tools/Extensions/gemfire-api" + GEMFIRE_VERSION + ".war";
    }
    else if (new File(gemfireHome + "/lib/gemfire-api" + GEMFIRE_VERSION + ".war").isFile()) {
      return gemfireHome + "/lib/gemfire-api" + GEMFIRE_VERSION + ".war";
    }
    else {
      return null;
    }
  }

  private boolean isRunningInTomcat() {
    return (System.getProperty("catalina.base") != null || System.getProperty("catalina.home") != null);
  }

  private boolean isWebApplicationAvailable(final String warFileLocation) {
    return !StringUtils.isBlank(warFileLocation);
  }

  private boolean isWebApplicationAvailable(final String... warFileLocations) {
    for (String warFileLocation : warFileLocations) {
      if (isWebApplicationAvailable(warFileLocation)) {
        return true;
      }
    }

    return false;
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
   * http://docs.oracle.com/javase/6/docs/technotes/guides/management/agent.html#gdfvq
   * https://blogs.oracle.com/jmxetc/entry/java_5_premain_rmi_connectors
   * https://blogs.oracle.com/jmxetc/entry/building_a_remotely_stoppable_connector
   * https://blogs.oracle.com/jmxetc/entry/jmx_connecting_through_firewalls_using
   */
  private void configureAndStart() throws IOException {
    // KIRK: I copied this from https://blogs.oracle.com/jmxetc/entry/java_5_premain_rmi_connectors
    //       we'll need to change this significantly but it's a starting point
    
    // get the port for RMI Registry and RMI Connector Server
    final int port = this.config.getJmxManagerPort();
    final String hostname;
    final InetAddress bindAddr;
    if (this.config.getJmxManagerBindAddress().equals("")) {
      hostname = SocketCreator.getLocalHost().getHostName();
      bindAddr = null;
    } else {
      hostname = this.config.getJmxManagerBindAddress();
      bindAddr = InetAddress.getByName(hostname);
    }
    
    final boolean ssl = this.config.getJmxManagerSSLEnabled();

    if (logger.isDebugEnabled()) {
      logger.debug("Starting jmx manager agent on port {}{}", port, (bindAddr != null ? (" bound to " + bindAddr) : "") + (ssl ? " using SSL" : ""));
    }

    final SocketCreator sc = SocketCreator.createNonDefaultInstance(ssl,
        this.config.getJmxManagerSSLRequireAuthentication(),
        this.config.getJmxManagerSSLProtocols(),
        this.config.getJmxManagerSSLCiphers(),
        this.config.getJmxSSLProperties());
    RMIClientSocketFactory csf = ssl ? new SslRMIClientSocketFactory() : null;//RMISocketFactory.getDefaultSocketFactory();
      //new GemFireRMIClientSocketFactory(sc, getLogger());
    RMIServerSocketFactory ssf = new GemFireRMIServerSocketFactory(sc, bindAddr);

    // Following is done to prevent rmi causing stop the world gcs
    System.setProperty("sun.rmi.dgc.server.gcInterval", Long.toString(Long.MAX_VALUE-1));
    
    // Create the RMI Registry using the SSL socket factories above.
    // In order to use a single port, we must use these factories 
    // everywhere, or nowhere. Since we want to use them in the JMX
    // RMI Connector server, we must also use them in the RMI Registry.
    // Otherwise, we wouldn't be able to use a single port.
    //
    // Start an RMI registry on port <port>.
    registry = LocateRegistry.createRegistry(port, csf, ssf);
    
    // Retrieve the PlatformMBeanServer.
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    
    

    // Environment map. KIRK: why is this declared as HashMap?
    final HashMap<String,Object> env = new HashMap<String,Object>();
    
    boolean integratedSecEnabled = System.getProperty("resource-authenticator") != null;
    if (integratedSecEnabled) {
      securityInterceptor = new ManagementInterceptor(logger);
      env.put(JMXConnectorServer.AUTHENTICATOR, securityInterceptor);
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
    
    
    
    // Manually creates and binds a JMX RMI Connector Server stub with the 
    // registry created above: the port we pass here is the port that can  
    // be specified in "service:jmx:rmi://"+hostname+":"+port - where the
    // RMI server stub and connection objects will be exported.
    // Here we choose to use the same port as was specified for the   
    // RMI Registry. We can do so because we're using \*the same\* client
    // and server socket factories, for the registry itself \*and\* for this
    // object.
    final RMIServerImpl stub = new RMIJRMPServerImpl(port, csf, ssf, env);
    
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
    final JMXServiceURL url = new JMXServiceURL(
        "service:jmx:rmi://"+hostname+":"+port+"/jndi/rmi://"+hostname+":"+port+"/jmxrmi");
    
    // Create an RMI connector server with the JMXServiceURL
    //    
    // KIRK: JDK 1.5 cannot use JMXConnectorServerFactory because of 
    // http://bugs.sun.com/view_bug.do?bug_id=5107423
    // but we're using JDK 1.6
    cs = new RMIConnectorServer(new JMXServiceURL("rmi",hostname,port),
          env,stub,mbs) {
      @Override
      public JMXServiceURL getAddress() { return url;}

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
    // This may be the 1.6 way of doing it but the problem is it does not use our "stub".
    //cs = JMXConnectorServerFactory.newJMXConnectorServer(url, env, mbs);
       
    if (integratedSecEnabled) {
      cs.setMBeanServerForwarder(securityInterceptor.getMBeanServerForwarder());
      logger.info("Starting RMI Connector with Security Interceptor");
    }  
    
    cs.start();
    if (logger.isDebugEnabled()) {
      logger.debug("Finished starting jmx manager agent.");
    }
    //System.out.println("Server started at: "+cs.getAddress());

    // Start the CleanThread daemon... KIRK: not sure what CleanThread is...
    //
    //final Thread clean = new CleanThread(cs);
    //clean.start();
  }
  
  private static class GemFireRMIClientSocketFactory implements RMIClientSocketFactory, Serializable {
    private static final long serialVersionUID = -7604285019188827617L;
    
    private /*final hack to prevent serialization*/ transient SocketCreator sc;
    
    public GemFireRMIClientSocketFactory(SocketCreator sc) {
      this.sc = sc;
    }

    @Override
    public Socket createSocket(String host, int port) throws IOException {
      return this.sc.connectForClient(host, port, 0/*no timeout*/);
    }
  };
  private static class GemFireRMIServerSocketFactory implements RMIServerSocketFactory, Serializable {
    private static final long serialVersionUID = -811909050641332716L;
    private /*final hack to prevent serialization*/ transient SocketCreator sc;
    private final InetAddress bindAddr;
    
    public GemFireRMIServerSocketFactory(SocketCreator sc, InetAddress bindAddr) {
      this.sc = sc;
      this.bindAddr = bindAddr;
    }

    @Override
    public ServerSocket createServerSocket(int port) throws IOException {
      return this.sc.createServerSocket(port, TCPConduit.getBackLog(), this.bindAddr);
    }
  };
}
