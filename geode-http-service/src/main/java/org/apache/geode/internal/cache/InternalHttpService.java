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
package org.apache.geode.internal.cache;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.handler.AllowSymLinkAliasChecker;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.webapp.WebAppContext;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.internal.HttpService;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.admin.SSLConfig;
import org.apache.geode.internal.net.SSLConfigurationFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.internal.SSLUtil;
import org.apache.geode.management.internal.beans.CacheServiceMBeanBase;

public class InternalHttpService implements HttpService {

  private static final Logger logger = LogService.getLogger();
  private Server httpServer;
  private String bindAddress = "0.0.0.0";
  private int port;
  private static final String FILE_PATH_SEPARATOR = System.getProperty("file.separator");
  private static final String USER_DIR = System.getProperty("user.dir");
  private static final String USER_NAME = System.getProperty("user.name");

  private static final String HTTPS = "https";

  private List<WebAppContext> webApps = new ArrayList<>();

  @Override
  public boolean init(Cache cache) {
    InternalDistributedSystem distributedSystem =
        (InternalDistributedSystem) cache.getDistributedSystem();
    DistributionConfig systemConfig = distributedSystem.getConfig();

    if (((InternalCache) cache).isClient()) {
      return false;
    }

    if (systemConfig.getHttpServicePort() == 0) {
      logger.info("HttpService is disabled with http-service-port = 0");
      return false;
    }

    try {
      createJettyServer(systemConfig.getHttpServiceBindAddress(),
          systemConfig.getHttpServicePort(),
          SSLConfigurationFactory.getSSLConfigForComponent(systemConfig,
              SecurableCommunicationChannel.WEB));
    } catch (Throwable ex) {
      logger.warn("Could not enable HttpService: {}", ex.getMessage());
      return false;
    }

    return true;
  }

  @VisibleForTesting
  public void createJettyServer(String bindAddress, int port, SSLConfig sslConfig) {
    this.httpServer = new Server();

    // Add a handler collection here, so that each new context adds itself
    // to this collection.
    httpServer.setHandler(new HandlerCollection(true));
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

      if (StringUtils.isNotBlank(sslConfig.getCiphers())
          && !"any".equalsIgnoreCase(sslConfig.getCiphers())) {
        sslContextFactory.setExcludeCipherSuites();
        sslContextFactory.setIncludeCipherSuites(SSLUtil.readArray(sslConfig.getCiphers()));
      }

      sslContextFactory.setSslContext(SSLUtil.createAndConfigureSSLContext(sslConfig, false));

      if (logger.isDebugEnabled()) {
        logger.debug(sslContextFactory.dump());
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

    logger.info("Enabled InternalHttpService on port {}", port);
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
      logger.info(
          String.format("unable to add %s webapp. Http service is not started on this member.",
              webAppContext));
      return;
    }

    WebAppContext webapp = new WebAppContext();
    webapp.setContextPath(webAppContext);
    webapp.setWar(warFilePath.toString());
    webapp.setParentLoaderPriority(false);

    // GEODE-7334: load all jackson classes from war file except jackson annotations
    webapp.getSystemClasspathPattern().add("com.fasterxml.jackson.annotation.");
    webapp.getServerClasspathPattern().add("com.fasterxml.jackson.",
        "-com.fasterxml.jackson.annotation.");

    webapp.setInitParameter("org.eclipse.jetty.servlet.Default.dirAllowed", "false");
    webapp.addAliasCheck(new AllowSymLinkAliasChecker());

    if (attributeNameValuePairs != null) {
      attributeNameValuePairs.forEach(webapp::setAttribute);
    }

    File tmpPath = new File(getWebAppBaseDirectory(webAppContext));
    tmpPath.mkdirs();
    webapp.setTempDirectory(tmpPath);
    logger.info("Adding webapp " + webAppContext);
    ((HandlerCollection) httpServer.getHandler()).addHandler(webapp);

    // if the server is not started yet start the server, otherwise, start the webapp alone
    if (!httpServer.isStarted()) {
      logger.info("Attempting to start HTTP service on port ({}) at bind-address ({})...",
          this.port, this.bindAddress);
      httpServer.start();
    } else {
      webapp.start();
    }
    webApps.add(webapp);
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

  @Override
  public void close() {
    if (this.httpServer == null) {
      return;
    }

    logger.debug("Stopping the HTTP service...");
    try {
      for (WebAppContext webapp : webApps) {
        webapp.stop();
      }
      this.httpServer.stop();
    } catch (Exception e) {
      logger.warn("Failed to stop the HTTP service because: {}", e.getMessage(), e);
    } finally {
      try {
        this.httpServer.destroy();
      } catch (Exception e) {
        logger.info("Failed to properly release resources held by the HTTP service: {}",
            e.getMessage(), e);
      } finally {
        this.httpServer = null;
      }
    }
  }
}
