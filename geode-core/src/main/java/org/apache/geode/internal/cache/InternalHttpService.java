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

import org.apache.geode.cache.internal.HttpService;
import org.apache.geode.internal.admin.SSLConfig;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.internal.SSLUtil;

public class InternalHttpService implements HttpService {

  private static final Logger logger = LogService.getLogger();
  private Server httpServer;
  private String bindAddress = "0.0.0.0";
  private int port;
  private SSLConfig sslConfig;
  private static final String FILE_PATH_SEPARATOR = System.getProperty("file.separator");
  private static final String USER_DIR = System.getProperty("user.dir");
  private static final String USER_NAME = System.getProperty("user.name");

  private static final String HTTPS = "https";

  public static final String SECURITY_SERVICE_SERVLET_CONTEXT_PARAM =
      "org.apache.geode.securityService";

  public static final String GEODE_SSLCONFIG_SERVLET_CONTEXT_PARAM = "org.apache.geode.sslConfig";
  public static final String CLUSTER_MANAGEMENT_SERVICE_CONTEXT_PARAM =
      "org.apache.geode.cluster.management.service";

  private List<WebAppContext> webApps = new ArrayList<>();

  public InternalHttpService(String bindAddress, int port, SSLConfig sslConfig) {
    if (port == 0) {
      return;
    }
    this.sslConfig = sslConfig;

    this.httpServer = new Server();

    // Add a handler collection here, so that each new context adds itself
    // to this collection.
    httpServer.setHandler(new HandlerCollection(true));
    ServerConnector connector = null;

    HttpConfiguration httpConfig = new HttpConfiguration();
    httpConfig.setSecureScheme(HTTPS);
    httpConfig.setSecurePort(port);

    if (sslConfig.isEnabled()) {
      SslContextFactory sslContextFactory = new SslContextFactory();

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

  public Server getHttpServer() {
    return httpServer;
  }

  @Override
  public synchronized void addWebApplication(String webAppContext, String warFilePath,
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
    webapp.setWar(warFilePath);
    webapp.setParentLoaderPriority(false);
    webapp.setInitParameter("org.eclipse.jetty.servlet.Default.dirAllowed", "false");
    webapp.addAliasCheck(new AllowSymLinkAliasChecker());

    if (attributeNameValuePairs != null) {
      attributeNameValuePairs.forEach((key, value) -> webapp.setAttribute(key, value));
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
    final String workingDirectory = USER_DIR.concat(FILE_PATH_SEPARATOR)
        .concat("GemFire_" + USER_NAME).concat(FILE_PATH_SEPARATOR).concat("services")
        .concat(FILE_PATH_SEPARATOR).concat("http").concat(FILE_PATH_SEPARATOR)
        .concat((StringUtils.isBlank(bindAddress)) ? "0.0.0.0" : bindAddress).concat("_")
        .concat(String.valueOf(port).concat(underscoredContext)).concat("_").concat(uuid);

    return workingDirectory;
  }

  @Override
  public void stop() {
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
      } catch (Exception ignore) {
        logger.info("Failed to properly release resources held by the HTTP service: {}",
            ignore.getMessage(), ignore);
      } finally {
        this.httpServer = null;
        System.clearProperty("catalina.base");
        System.clearProperty("catalina.home");
      }
    }
  }
}
