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
package com.gemstone.gemfire.management.internal;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.webapp.WebAppContext;

import com.gemstone.gemfire.GemFireConfigException;
import com.gemstone.gemfire.internal.lang.StringUtils;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * @since GemFire 8.1
 */
@SuppressWarnings("unused")
public class JettyHelper {
  private static final Logger logger = LogService.getLogger();

  private static final String FILE_PATH_SEPARATOR = System.getProperty(
      "file.separator");
  private static final String USER_DIR = System.getProperty("user.dir");

  private static final String USER_NAME = System.getProperty("user.name");
  
  private static final String HTTPS = "https";

  private static String bindAddress = "0.0.0.0";

  private static int port = 0;
  
  public static Server initJetty(final String bindAddress, final int port, boolean useSSL,
      boolean needClientAuth, String protocols, String ciphers, Properties sysProps) throws Exception {

    final Server jettyServer = new Server();

    // Add a handler collection here, so that each new context adds itself
    // to this collection.
    jettyServer.setHandler(new HandlerCollection());
    ServerConnector connector = null;
    
    HttpConfiguration httpConfig = new HttpConfiguration();
    httpConfig.setSecureScheme(HTTPS);
    httpConfig.setSecurePort(port);

    if (useSSL) {
      SslContextFactory sslContextFactory = new SslContextFactory();
      
      sslContextFactory.setNeedClientAuth(needClientAuth);
   
      if (!StringUtils.isBlank(ciphers) && !"any".equalsIgnoreCase(ciphers)) {
        //If use has mentioned "any" let the SSL layer decide on the ciphers
        sslContextFactory.setIncludeCipherSuites(SSLUtil.readArray(ciphers));
      }

      String protocol = SSLUtil.getSSLAlgo(SSLUtil.readArray(protocols));
      if (protocol != null) {
        sslContextFactory.setProtocol(protocol);
      } else {
        logger.warn(ManagementStrings.SSL_PROTOCOAL_COULD_NOT_BE_DETERMINED);
      }
      

      if (StringUtils.isBlank(sysProps.getProperty("javax.net.ssl.keyStore"))) {
        throw new GemFireConfigException("Key store can't be empty if SSL is enabled for HttpService");
      }

      sslContextFactory.setKeyStorePath(sysProps.getProperty("javax.net.ssl.keyStore"));

      if (!StringUtils.isBlank(sysProps.getProperty("javax.net.ssl.keyStoreType"))) {
        sslContextFactory.setKeyStoreType(sysProps.getProperty("javax.net.ssl.keyStoreType"));
      }

      if (!StringUtils.isBlank(sysProps.getProperty("javax.net.ssl.keyStorePassword"))){
        sslContextFactory.setKeyStorePassword(sysProps.getProperty("javax.net.ssl.keyStorePassword"));
      }

      if (!StringUtils.isBlank(sysProps.getProperty("javax.net.ssl.trustStore"))){
        sslContextFactory.setTrustStorePath(sysProps.getProperty("javax.net.ssl.trustStore"));
      }

      if (!StringUtils.isBlank(sysProps.getProperty("javax.net.ssl.trustStorePassword"))){
        sslContextFactory.setTrustStorePassword(sysProps.getProperty("javax.net.ssl.trustStorePassword"));
      }
      

      httpConfig.addCustomizer(new SecureRequestCustomizer());

      //Somehow With HTTP_2.0 Jetty throwing NPE. Need to investigate further whether all GemFire web application(Pulse, REST) can do with HTTP_1.1
      connector = new ServerConnector(jettyServer, new SslConnectionFactory(sslContextFactory,
          HttpVersion.HTTP_1_1.asString()), new HttpConnectionFactory(httpConfig));
      

      connector.setPort(port);
    } else {
      connector = new ServerConnector(jettyServer, new HttpConnectionFactory(httpConfig));
     
      connector.setPort(port);
    }

    jettyServer.setConnectors(new Connector[] { connector});
    
    if (!StringUtils.isBlank(bindAddress)) {
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

  public static Server addWebApplication(final Server jetty,
      final String webAppContext, final String warFilePath) {
    WebAppContext webapp = new WebAppContext();
    webapp.setContextPath(webAppContext);
    webapp.setWar(warFilePath);
    webapp.setParentLoaderPriority(false);
    webapp.setInitParameter("org.eclipse.jetty.servlet.Default.dirAllowed", "false");

    File tmpPath = new File(getWebAppBaseDirectory(webAppContext));
    tmpPath.mkdirs();
    webapp.setTempDirectory(tmpPath);

    ((HandlerCollection) jetty.getHandler()).addHandler(webapp);

    return jetty;
  }



  private static String getWebAppBaseDirectory(final String context) {
    String underscoredContext = context.replace("/", "_");
    final String workingDirectory = USER_DIR
        .concat(FILE_PATH_SEPARATOR)
        .concat("GemFire_" + USER_NAME)
        .concat(FILE_PATH_SEPARATOR)
        .concat("services")
        .concat(FILE_PATH_SEPARATOR)
        .concat("http")
        .concat(FILE_PATH_SEPARATOR)
        .concat(
            (StringUtils.isBlank(bindAddress)) ? "0.0.0.0" : bindAddress)
        .concat("_")
        .concat(String.valueOf(port)
        .concat(underscoredContext));

    return workingDirectory;
  }

  private static final CountDownLatch latch = new CountDownLatch(1);

  private static String normalizeWebAppArchivePath(
      final String webAppArchivePath) {
    return (webAppArchivePath.startsWith(File.separator) ? new File(
        webAppArchivePath) :
        new File(".", webAppArchivePath)).getAbsolutePath();
  }

  private static String normalizeWebAppContext(final String webAppContext) {
    return (webAppContext.startsWith(
        "/") ? webAppContext : "/" + webAppContext);
  }

  public static void main(final String... args) throws Exception {
    if (args.length > 1) {
      System.out.printf("Temporary Directory @ ($1%s)%n", USER_DIR);

      final Server jetty = JettyHelper.initJetty(null, 8090, false, false, null, null, null);

      for (int index = 0; index < args.length; index += 2) {
        final String webAppContext = args[index];
        final String webAppArchivePath = args[index + 1];

        JettyHelper.addWebApplication(jetty,
            normalizeWebAppContext(webAppContext),
            normalizeWebAppArchivePath(webAppArchivePath));
      }

      JettyHelper.startJetty(jetty);
      latch.await();
    } else {
      System.out.printf(
          "usage:%n>java com.gemstone.gemfire.management.internal.TomcatHelper <web-app-context> <war-file-path> [<web-app-context> <war-file-path>]*");
    }
  }

}
