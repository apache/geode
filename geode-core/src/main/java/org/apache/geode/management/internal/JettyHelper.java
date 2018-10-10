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

import java.io.File;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.lang.StringUtils;
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

import org.apache.geode.GemFireConfigException;
import org.apache.geode.internal.admin.SSLConfig;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.security.SecurityService;

/**
 * @since GemFire 8.1
 */
@SuppressWarnings("unused")
public class JettyHelper {

  private static final Logger logger = LogService.getLogger();

  private static final String FILE_PATH_SEPARATOR = System.getProperty("file.separator");
  private static final String USER_DIR = System.getProperty("user.dir");
  private static final String USER_NAME = System.getProperty("user.name");

  private static final String HTTPS = "https";

  private static String bindAddress = "0.0.0.0";

  private static int port = 0;

  public static final String SECURITY_SERVICE_SERVLET_CONTEXT_PARAM =
      "org.apache.geode.securityService";

  private static final String GEODE_SSLCONFIG_SERVLET_CONTEXT_PARAM = "org.apache.geode.sslConfig";

  public static Server initJetty(final String bindAddress, final int port, SSLConfig sslConfig) {

    final Server jettyServer = new Server();

    // Add a handler collection here, so that each new context adds itself
    // to this collection.
    jettyServer.setHandler(new HandlerCollection());
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

      String protocol = SSLUtil.getSSLAlgo(SSLUtil.readArray(sslConfig.getProtocols()));
      if (protocol != null) {
        sslContextFactory.setProtocol(protocol);
      } else {
        logger.warn("SSL Protocol could not be determined. SSL settings might not work correctly");
      }


      if (StringUtils.isBlank(sslConfig.getKeystore())) {
        throw new GemFireConfigException(
            "Key store can't be empty if SSL is enabled for HttpService");
      }

      sslContextFactory.setKeyStorePath(sslConfig.getKeystore());

      if (StringUtils.isNotBlank(sslConfig.getKeystoreType())) {
        sslContextFactory.setKeyStoreType(sslConfig.getKeystoreType());
      }

      if (StringUtils.isNotBlank(sslConfig.getKeystorePassword())) {
        sslContextFactory.setKeyStorePassword(sslConfig.getKeystorePassword());
      }

      if (StringUtils.isNotBlank(sslConfig.getTruststore())) {
        sslContextFactory.setTrustStorePath(sslConfig.getTruststore());
      }

      if (StringUtils.isNotBlank(sslConfig.getTruststorePassword())) {
        sslContextFactory.setTrustStorePassword(sslConfig.getTruststorePassword());
      }

      if (StringUtils.isNotBlank(sslConfig.getTruststoreType())) {
        sslContextFactory.setTrustStoreType(sslConfig.getTruststoreType());
      }

      if (logger.isDebugEnabled()) {
        logger.debug(sslContextFactory.dump());
      }
      httpConfig.addCustomizer(new SecureRequestCustomizer());

      // Somehow With HTTP_2.0 Jetty throwing NPE. Need to investigate further whether all GemFire
      // web application(Pulse, REST) can do with HTTP_1.1
      connector = new ServerConnector(jettyServer,
          new SslConnectionFactory(sslContextFactory, HttpVersion.HTTP_1_1.asString()),
          new HttpConnectionFactory(httpConfig));


      connector.setPort(port);
    } else {
      connector = new ServerConnector(jettyServer, new HttpConnectionFactory(httpConfig));

      connector.setPort(port);
    }

    jettyServer.setConnectors(new Connector[] {connector});

    if (StringUtils.isNotBlank(bindAddress)) {
      connector.setHost(bindAddress);
    }


    if (bindAddress != null && !bindAddress.isEmpty()) {
      JettyHelper.bindAddress = bindAddress;
    }

    JettyHelper.port = port;

    return jettyServer;
  }


  public static Server startJetty(final Server jetty) throws Exception {
    jetty.start();
    return jetty;
  }

  public static Server addWebApplication(final Server jetty, final String webAppContext,
      final String warFilePath, SecurityService securityService, Properties sslConfig) {
    WebAppContext webapp = new WebAppContext();
    webapp.setContextPath(webAppContext);
    webapp.setWar(warFilePath);
    webapp.setParentLoaderPriority(false);
    webapp.setInitParameter("org.eclipse.jetty.servlet.Default.dirAllowed", "false");
    webapp.setAttribute(SECURITY_SERVICE_SERVLET_CONTEXT_PARAM, securityService);
    webapp.addAliasCheck(new AllowSymLinkAliasChecker());

    // This is only required for Pulse because in embedded mode, with SSL enabled, Pulse needs to
    // know how to make SSL RMI connections.
    webapp.setAttribute(GEODE_SSLCONFIG_SERVLET_CONTEXT_PARAM, sslConfig);

    File tmpPath = new File(getWebAppBaseDirectory(webAppContext));
    tmpPath.mkdirs();
    webapp.setTempDirectory(tmpPath);

    ((HandlerCollection) jetty.getHandler()).addHandler(webapp);

    return jetty;
  }

  private static String getWebAppBaseDirectory(final String context) {
    String underscoredContext = context.replace("/", "_");
    String uuid = UUID.randomUUID().toString().substring(0, 8);
    final String workingDirectory = USER_DIR.concat(FILE_PATH_SEPARATOR)
        .concat("GemFire_" + USER_NAME).concat(FILE_PATH_SEPARATOR).concat("services")
        .concat(FILE_PATH_SEPARATOR).concat("http").concat(FILE_PATH_SEPARATOR)
        .concat((StringUtils.isBlank(bindAddress)) ? "0.0.0.0" : bindAddress).concat("_")
        .concat(String.valueOf(port).concat(underscoredContext)).concat("_").concat(uuid);

    return workingDirectory;
  }

  private static final CountDownLatch latch = new CountDownLatch(1);

  private static String normalizeWebAppArchivePath(final String webAppArchivePath) {
    return (webAppArchivePath.startsWith(File.separator) ? new File(webAppArchivePath)
        : new File(".", webAppArchivePath)).getAbsolutePath();
  }

  private static String normalizeWebAppContext(final String webAppContext) {
    return (webAppContext.startsWith("/") ? webAppContext : "/" + webAppContext);
  }

}
