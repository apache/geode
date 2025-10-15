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
package org.apache.geode.internal.cache.http.service;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletContextEvent;
import jakarta.servlet.ServletContextListener;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;
import org.eclipse.jetty.ee10.annotations.AnnotationConfiguration;
import org.eclipse.jetty.ee10.servlet.ListenerHolder;
import org.eclipse.jetty.ee10.servlet.Source;
import org.eclipse.jetty.ee10.webapp.WebAppContext;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.SymlinkAllowedResourceAliasChecker;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.internal.HttpService;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.CacheService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.net.SSLConfig;
import org.apache.geode.internal.net.SSLConfigurationFactory;
import org.apache.geode.internal.net.SSLUtil;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.internal.beans.CacheServiceMBeanBase;

public class InternalHttpService implements HttpService {

  private static final Logger logger = LogService.getLogger();

  // Markers enable filtering logs by concern in production (e.g., "grep HTTP_LIFECYCLE logs.txt")
  // and support structured log aggregation systems. Without markers, operators must parse
  // unstructured text to separate lifecycle events from configuration details.
  private static final Marker LIFECYCLE = MarkerManager.getMarker("HTTP_LIFECYCLE");
  private static final Marker WEBAPP = MarkerManager.getMarker("HTTP_WEBAPP");
  private static final Marker SERVLET_CONTEXT = MarkerManager.getMarker("SERVLET_CONTEXT");
  private static final Marker CONFIG = MarkerManager.getMarker("HTTP_CONFIG");
  private static final Marker SECURITY = MarkerManager.getMarker("HTTP_SECURITY");

  private Server httpServer;
  private String bindAddress = "0.0.0.0";
  private int port;
  private static final String FILE_PATH_SEPARATOR = System.getProperty("file.separator");
  private static final String USER_DIR = System.getProperty("user.dir");
  private static final String USER_NAME = System.getProperty("user.name");

  private static final String HTTPS = "https";

  private final List<WebAppContext> webApps = new ArrayList<>();

  /**
   * Bridges WebAppContext and ServletContext attribute namespaces in Jetty 12.
   *
   * <p>
   * Why needed: In Jetty 12, WebAppContext.setAttribute() stores attributes in the webapp's
   * context, but Spring's ServletContextAware beans (like LoginHandlerInterceptor) retrieve
   * from ServletContext.getAttribute(). These are separate namespaces that don't auto-sync.
   *
   * <p>
   * Timing: contextInitialized() is invoked BEFORE Spring's DispatcherServlet initializes,
   * guaranteeing attributes are present when Spring beans request them during dependency injection.
   * Without this, SecurityService would be null in LoginHandlerInterceptor, causing 503 errors.
   */
  private static class ServletContextAttributeListener implements ServletContextListener {
    private static final Logger logger = LogService.getLogger();
    private final Map<String, Object> attributes;
    private final String webAppContext;

    public ServletContextAttributeListener(Map<String, Object> attributes, String webAppContext) {
      this.attributes = attributes;
      this.webAppContext = webAppContext;
    }

    @Override
    public void contextInitialized(ServletContextEvent sce) {
      ServletContext ctx = sce.getServletContext();

      logger.info(SERVLET_CONTEXT, "Initializing ServletContext: {}",
          new LogContext()
              .add("webapp", webAppContext)
              .add("attributeCount", attributes.size()));

      // Copy each attribute to ServletContext so Spring dependency injection can find them.
      // Without this, SecurityService lookup in LoginHandlerInterceptor returns null.
      attributes.forEach((key, value) -> {
        ctx.setAttribute(key, value);
        if (logger.isDebugEnabled()) {
          logger.debug(SERVLET_CONTEXT, "Set ServletContext attribute: key={}, value={}",
              key, value);
        }
      });

      logger.info(SERVLET_CONTEXT, "ServletContext initialized: {}",
          new LogContext()
              .add("webapp", webAppContext)
              .add("attributesTransferred", attributes.size()));
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
      if (logger.isDebugEnabled()) {
        logger.debug(SERVLET_CONTEXT, "ServletContext destroyed: webapp={}", webAppContext);
      }
    }
  }

  @Override
  public boolean init(Cache cache) {
    InternalDistributedSystem distributedSystem =
        (InternalDistributedSystem) cache.getDistributedSystem();
    DistributionConfig systemConfig = distributedSystem.getConfig();

    if (((InternalCache) cache).isClient()) {
      if (logger.isDebugEnabled()) {
        logger.debug(LIFECYCLE, "HTTP service not initialized: client cache");
      }
      return false;
    }

    if (systemConfig.getHttpServicePort() == 0) {
      logger.info(CONFIG, "HTTP service disabled: http-service-port=0");
      return false;
    }

    try {
      createJettyServer(systemConfig.getHttpServiceBindAddress(),
          systemConfig.getHttpServicePort(),
          SSLConfigurationFactory.getSSLConfigForComponent(systemConfig,
              SecurableCommunicationChannel.WEB));
    } catch (Throwable ex) {
      logger.warn(LIFECYCLE, "Failed to enable HTTP service: {}", ex.getMessage());
      return false;
    }

    return true;
  }

  @VisibleForTesting
  public void createJettyServer(String bindAddress, int port, SSLConfig sslConfig) {
    httpServer = new Server();

    // Jetty 12: Use Handler.Sequence instead of HandlerCollection
    // Handler.Sequence is a dynamic list of handlers
    httpServer.setHandler(new Handler.Sequence());
    final ServerConnector connector;

    HttpConfiguration httpConfig = new HttpConfiguration();
    httpConfig.setSecureScheme(HTTPS);
    httpConfig.setSecurePort(port);

    if (sslConfig.isEnabled()) {
      SslContextFactory.Server sslContextFactory = new SslContextFactory.Server();

      if (StringUtils.isNotBlank(sslConfig.getAlias())) {
        sslContextFactory.setCertAlias(sslConfig.getAlias());
      }

      sslContextFactory.setNeedClientAuth(sslConfig.isRequireAuth());

      if (!sslConfig.isAnyCiphers()) {
        sslContextFactory.setExcludeCipherSuites();
        sslContextFactory.setIncludeCipherSuites(sslConfig.getCiphersAsStringArray());
      }

      sslContextFactory.setSslContext(SSLUtil.createAndConfigureSSLContext(sslConfig, false));

      if (logger.isDebugEnabled()) {
        logger.debug(SECURITY, "SSL context factory configuration: {}", sslContextFactory.dump());
      }
      httpConfig.addCustomizer(new SecureRequestCustomizer());

      // Somehow With HTTP_2.0 Jetty throwing NPE. Need to investigate further whether all GemFire
      // web application(Pulse, REST) can do with HTTP_1.1
      connector = new ServerConnector(httpServer,
          new SslConnectionFactory(sslContextFactory, HttpVersion.HTTP_1_1.asString()),
          new HttpConnectionFactory(httpConfig));

      connector.setPort(port);
    } else {
      connector = new ServerConnector(httpServer, new HttpConnectionFactory(httpConfig));

      connector.setPort(port);
    }

    httpServer.setConnectors(new Connector[] {connector});

    if (StringUtils.isNotBlank(bindAddress)) {
      connector.setHost(bindAddress);
    }

    if (bindAddress != null && !bindAddress.isEmpty()) {
      this.bindAddress = bindAddress;
    }
    this.port = port;

    logger.info(LIFECYCLE, "HTTP service initialized: {}",
        new LogContext()
            .add("port", port)
            .add("bindAddress",
                bindAddress != null && !bindAddress.isEmpty() ? bindAddress : "0.0.0.0")
            .add("ssl", sslConfig.isEnabled()));
  }

  @Override
  public Class<? extends CacheService> getInterface() {
    return HttpService.class;
  }

  @Override
  public CacheServiceMBeanBase getMBean() {
    return null;
  }

  public Server getHttpServer() {
    return httpServer;
  }

  @Override
  public synchronized void addWebApplication(String webAppContext, Path warFilePath,
      Map<String, Object> attributeNameValuePairs)
      throws Exception {
    if (httpServer == null) {
      logger.warn(WEBAPP, "Cannot add webapp, HTTP service not started: webapp={}", webAppContext);
      return;
    }

    logger.info(WEBAPP, "Adding webapp {}", webAppContext);

    WebAppContext webapp = new WebAppContext();
    webapp.setContextPath(webAppContext);
    webapp.setWar(warFilePath.toString());

    // Required for Spring Boot initialization: AnnotationConfiguration triggers Jetty's annotation
    // scanning during webapp.configure(), which discovers SpringServletContainerInitializer via
    // ServiceLoader from META-INF/services. Without this, Spring's WebApplicationInitializer
    // chain never starts, causing 404 errors for all REST endpoints.
    // Reference: jetty-ee10-demos/embedded/src/main/java/ServerWithAnnotations.java
    webapp.addConfiguration(new AnnotationConfiguration());

    // Child-first classloading prevents parent classloader's Jackson from conflicting with
    // webapp's bundled version, avoiding NoSuchMethodError during JSON serialization.
    webapp.setParentLoaderPriority(false);

    // GEODE-7334: load all jackson classes from war file except jackson annotations
    // Jetty 12: Attribute names changed to ee10.webapp namespace
    webapp.setAttribute("org.eclipse.jetty.ee10.webapp.ContainerIncludeJarPattern",
        ".*/jakarta\\.servlet-api-[^/]*\\.jar$|" +
            ".*/jakarta\\.servlet\\.jsp\\.jstl-.*\\.jar$|" +
            ".*/com\\.fasterxml\\.jackson\\.annotation\\..*\\.jar$");
    webapp.setAttribute("org.eclipse.jetty.ee10.webapp.WebInfIncludeJarPattern",
        ".*/com\\.fasterxml\\.jackson\\.(?!annotation).*\\.jar$");

    // add the member's working dir as the extra classpath
    webapp.setExtraClasspath(new File(".").getAbsolutePath());

    webapp.setInitParameter("org.eclipse.jetty.servlet.Default.dirAllowed", "false");
    webapp.addAliasCheck(new SymlinkAllowedResourceAliasChecker(webapp));

    // Store attributes on WebAppContext for backward compatibility
    if (attributeNameValuePairs != null) {
      attributeNameValuePairs.forEach(webapp::setAttribute);

      // Listener must be registered as Source.EMBEDDED to execute during ServletContext
      // initialization, BEFORE DispatcherServlet starts. This timing guarantees Spring's
      // dependency injection finds SecurityService when initializing LoginHandlerInterceptor.
      // Using Source.JAVAX_API or adding via web.xml would execute too late in the lifecycle.
      // Pattern reference: jetty-ee10/jetty-ee10-servlet/OneServletContext.java
      ListenerHolder listenerHolder = new ListenerHolder(Source.EMBEDDED);
      listenerHolder.setListener(
          new ServletContextAttributeListener(attributeNameValuePairs, webAppContext));
      webapp.getServletHandler().addListener(listenerHolder);
    }

    File tmpPath = new File(getWebAppBaseDirectory(webAppContext));
    tmpPath.mkdirs();
    webapp.setTempDirectory(tmpPath);

    if (logger.isDebugEnabled()) {
      ClassLoader webappClassLoader = webapp.getClassLoader();
      ClassLoader parentClassLoader =
          (webappClassLoader != null) ? webappClassLoader.getParent() : null;
      logger.debug(CONFIG, "Webapp configuration: {}",
          new LogContext()
              .add("context", webAppContext)
              .add("tempDir", tmpPath.getAbsolutePath())
              .add("parentLoaderPriority", webapp.isParentLoaderPriority())
              .add("webappClassLoader", webappClassLoader)
              .add("parentClassLoader", parentClassLoader)
              .add("annotationConfigEnabled", true)
              .add("servletContextListenerAdded", attributeNameValuePairs != null));
    }

    // In Jetty 12, Handler.Sequence replaced HandlerCollection for dynamic handler lists
    ((Handler.Sequence) httpServer.getHandler()).addHandler(webapp);

    // Server start deferred to restartHttpServer() to batch all webapp configurations,
    // avoiding multiple restart cycles and ensuring all webapps initialize together.
    webApps.add(webapp);

    logger.info(WEBAPP, "Webapp deployed successfully: {}",
        new LogContext()
            .add("context", webAppContext)
            .add("totalWebapps", webApps.size())
            .add("servletContextListener", attributeNameValuePairs != null));
  }

  private String getWebAppBaseDirectory(final String context) {
    String underscoredContext = context.replace("/", "_");
    String uuid = UUID.randomUUID().toString().substring(0, 8);

    return USER_DIR.concat(FILE_PATH_SEPARATOR)
        .concat("GemFire_" + USER_NAME).concat(FILE_PATH_SEPARATOR).concat("services")
        .concat(FILE_PATH_SEPARATOR).concat("http").concat(FILE_PATH_SEPARATOR)
        .concat((StringUtils.isBlank(bindAddress)) ? "0.0.0.0" : bindAddress).concat("_")
        .concat(String.valueOf(port).concat(underscoredContext)).concat("_").concat(uuid);
  }

  /**
   * Forces complete Jetty configuration lifecycle for all webapps to trigger annotation scanning.
   *
   * <p>
   * Why needed: AnnotationConfiguration.configure() only runs during server.start(), not during
   * addHandler(). Without this restart, ServletContainerInitializer discovery via ServiceLoader
   * never occurs, causing Spring initialization to fail silently with 404s on all endpoints.
   *
   * <p>
   * Must be called after all addWebApplication() calls to batch configurations and avoid
   * multiple restart cycles.
   */
  public synchronized void restartHttpServer() throws Exception {
    if (httpServer == null) {
      logger.warn(LIFECYCLE, "Cannot restart HTTP server: server not initialized");
      return;
    }

    boolean isStarted = httpServer.isStarted();
    int webappCount = webApps.size();

    logger.info(LIFECYCLE, "{} HTTP server: {}",
        isStarted ? "Restarting" : "Starting",
        new LogContext()
            .add("webappCount", webappCount)
            .add("firstStart", !isStarted));

    if (logger.isDebugEnabled()) {
      logger.debug(LIFECYCLE, "Jetty lifecycle will: {} -> {} -> {} -> {}",
          "loadConfigurations", "preConfigure", "configure (ServletContainerInitializer discovery)",
          "start");
    }

    if (isStarted) {
      // Server is running - stop it before restarting
      if (logger.isDebugEnabled()) {
        logger.debug(LIFECYCLE, "Stopping running server before restart");
      }
      httpServer.stop();
    }

    httpServer.start();

    logger.info(LIFECYCLE, "HTTP server {} successfully: {}",
        isStarted ? "restarted" : "started",
        new LogContext()
            .add("webappCount", webappCount)
            .add("port", port)
            .add("bindAddress", bindAddress));
  }

  @Override
  public void close() {
    if (httpServer == null) {
      return;
    }

    if (logger.isDebugEnabled()) {
      logger.debug(LIFECYCLE, "Stopping HTTP service: webappCount={}", webApps.size());
    }

    try {
      for (WebAppContext webapp : webApps) {
        webapp.stop();
      }
      httpServer.stop();
    } catch (Exception e) {
      logger.warn(LIFECYCLE, "Failed to stop HTTP service: {}", e.getMessage(), e);
    } finally {
      try {
        httpServer.destroy();
      } catch (Exception e) {
        logger.warn(LIFECYCLE, "Failed to release HTTP service resources: {}",
            e.getMessage(), e);
      } finally {
        httpServer = null;
      }
    }
  }

  /**
   * Produces structured key=value log output for machine parsing and log aggregation.
   *
   * <p>
   * Why needed: Operations teams need to filter logs programmatically (e.g., find all
   * "port=7070" occurrences) and feed structured data to log analysis tools. Free-form
   * text logging forces fragile regex parsing and makes automated alerting unreliable.
   *
   * <p>
   * Example:
   *
   * <pre>
   * logger.info(LIFECYCLE, "Server started: {}",
   *     new LogContext()
   *         .add("port", port)
   *         .add("ssl", sslEnabled)
   *         .add("webappCount", webApps.size()));
   * </pre>
   *
   * Output: "Server started: port=7070, ssl=true, webappCount=3"
   */
  private static class LogContext {
    private final java.util.LinkedHashMap<String, Object> context = new java.util.LinkedHashMap<>();

    public LogContext add(String key, Object value) {
      context.put(key, value);
      return this;
    }

    @Override
    public String toString() {
      return context.entrySet().stream()
          .map(e -> e.getKey() + "=" + e.getValue())
          .collect(java.util.stream.Collectors.joining(", "));
    }
  }
}
