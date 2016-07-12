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

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.GemFireVersion;
import com.gemstone.gemfire.internal.net.SocketCreator;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.InternalRegionArguments;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.management.ManagementService;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;

import java.net.UnknownHostException;

/**
 * Agent implementation that controls the HTTP server end points used for REST
 * clients to connect gemfire data node.
 * <p>
 * The RestAgent is used to start http service in embedded mode on any non
 * manager data node with developer REST APIs service enabled.
 *
 * @since GemFire 8.0
 */
public class RestAgent {
  private static final Logger logger = LogService.getLogger();

  private boolean running = false;
  private final DistributionConfig config;

  public RestAgent(DistributionConfig config) {
    this.config = config;
  }

  public synchronized boolean isRunning() {
    return this.running;
  }

  private boolean isManagementRestServiceRunning(GemFireCacheImpl cache) {
    final SystemManagementService managementService = (SystemManagementService) ManagementService
        .getManagementService(cache);
    return (managementService.getManagementAgent() != null && managementService
        .getManagementAgent().isHttpServiceRunning());

  }

  public synchronized void start(GemFireCacheImpl cache) {
    if (!this.running && this.config.getHttpServicePort() != 0
        && !isManagementRestServiceRunning(cache)) {
      try {
        startHttpService();
        this.running = true;
        cache.setRESTServiceRunning(true);

        // create region to hold query information (queryId, queryString). Added
        // for the developer REST APIs
        RestAgent.createParameterizedQueryRegion();

      } catch (RuntimeException e) {
        logger.debug(e.getMessage(), e);
      }
    }

  }

  public synchronized void stop() {
    if (this.running) {
      stopHttpService();
      if (logger.isDebugEnabled()) {
        logger.debug("Gemfire Rest Http service stopped");
      }
      this.running = false;
    } else {
      if (logger.isDebugEnabled()) {
        logger.debug("Attempt to stop Gemfire Rest Http service which is not running");
      }
    }
  }

  private Server httpServer;
  private final String GEMFIRE_VERSION = GemFireVersion.getGemFireVersion();
  private AgentUtil agentUtil = new AgentUtil(GEMFIRE_VERSION);

  private boolean isRunningInTomcat() {
    return (System.getProperty("catalina.base") != null || System.getProperty("catalina.home") != null);
  }

  // Start HTTP service in embedded mode
  public void startHttpService() {
    // TODO: add a check that will make sure that we start HTTP service on
    // non-manager data node
    String httpServiceBindAddress = getBindAddressForHttpService();
    logger.info("Attempting to start HTTP service on port ({}) at bind-address ({})...",
        this.config.getHttpServicePort(), httpServiceBindAddress);

    // Find the developer REST WAR file
    final String gemfireAPIWar = agentUtil.findWarLocation("geode-web-api");
    if (gemfireAPIWar == null) {
      logger.info("Unable to find GemFire Developer REST API WAR file; the Developer REST Interface for GemFire will not be accessible.");
    }

    try {
      // Check if we're already running inside Tomcat
      if (isRunningInTomcat()) {
        logger.warn("Detected presence of catalina system properties. HTTP service will not be started. To enable the GemFire Developer REST API, please deploy the /geode-web-api WAR file in your application server."); 
      } else if (agentUtil.isWebApplicationAvailable(gemfireAPIWar)) {

        final int port = this.config.getHttpServicePort();

        this.httpServer = JettyHelper.initJetty(httpServiceBindAddress, port,
            this.config.getHttpServiceSSLEnabled(),
            this.config.getHttpServiceSSLRequireAuthentication(),
            this.config.getHttpServiceSSLProtocols(), this.config.getHttpServiceSSLCiphers(),
            this.config.getHttpServiceSSLProperties());

        this.httpServer = JettyHelper.addWebApplication(httpServer, "/gemfire-api", gemfireAPIWar);

        if (logger.isDebugEnabled()) {
          logger.info("Starting HTTP embedded server on port ({}) at bind-address ({})...",
              ((ServerConnector) this.httpServer.getConnectors()[0]).getPort(), httpServiceBindAddress);
        }

        this.httpServer = JettyHelper.startJetty(this.httpServer);
        logger.info("HTTP service started successfully...!!");
      }
    } catch (Exception e) {
      stopHttpService();// Jetty needs to be stopped even if it has failed to
                        // start. Some of the threads are left behind even if
                        // server.start() fails due to an exception
      throw new RuntimeException("HTTP service failed to start due to " + e.getMessage());
    }
  }

  private String getBindAddressForHttpService() {
    java.lang.String bindAddress = this.config.getHttpServiceBindAddress();
    if (StringUtils.isBlank(bindAddress)) {
      if (StringUtils.isBlank(this.config.getServerBindAddress())) {
        if (StringUtils.isBlank(this.config.getBindAddress())) {
          try {
            bindAddress = SocketCreator.getLocalHost().getHostAddress();
            logger.info("RestAgent.getBindAddressForHttpService.localhost: " + SocketCreator.getLocalHost().getHostAddress());
          } catch (UnknownHostException e) {
            logger.error("LocalHost could not be found.", e);
            return bindAddress;
          }
        } else {
          bindAddress = this.config.getBindAddress();
        }
      } else {
        bindAddress = this.config.getServerBindAddress();
      }
    }
    return bindAddress;
  }

  private void stopHttpService() {
    if (this.httpServer != null) {
      logger.info("Stopping the HTTP service...");
      try {
        this.httpServer.stop();
      } catch (Exception e) {
        logger.warn("Failed to stop the HTTP service because: {}", e.getMessage(), e);
      } finally {
        try {
          this.httpServer.destroy();
        } catch (Exception ignore) {
          logger.error("Failed to properly release resources held by the HTTP service: {}",
              ignore.getMessage(), ignore);
        } finally {
          this.httpServer = null;
          System.clearProperty("catalina.base");
          System.clearProperty("catalina.home");
        }
      }
    }
  }

  /**
   * This method will create a REPLICATED region named _ParameterizedQueries__.
   * In developer REST APIs, this region will be used to store the queryId and
   * queryString as a key and value respectively.
   */
  public static void createParameterizedQueryRegion() {
    try {
      if (logger.isDebugEnabled()) {
        logger.debug("Starting creation of  __ParameterizedQueries__ region");
      }
      GemFireCacheImpl cache = (GemFireCacheImpl) CacheFactory.getAnyInstance();
      if (cache != null) {
        // cache.getCacheConfig().setPdxReadSerialized(true);
        final InternalRegionArguments regionArguments = new InternalRegionArguments();
        regionArguments.setIsUsedForMetaRegion(true);
        final AttributesFactory<String, String> attributesFactory = new AttributesFactory<String, String>();

        attributesFactory.setConcurrencyChecksEnabled(false);
        attributesFactory.setDataPolicy(DataPolicy.REPLICATE);
        attributesFactory.setKeyConstraint(String.class);
        attributesFactory.setScope(Scope.DISTRIBUTED_NO_ACK);
        attributesFactory.setStatisticsEnabled(false);
        attributesFactory.setValueConstraint(String.class);

        final RegionAttributes<String, String> regionAttributes = attributesFactory.create();

        cache.createVMRegion("__ParameterizedQueries__", regionAttributes, regionArguments);
        if (logger.isDebugEnabled()) {
          logger.debug("Successfully created __ParameterizedQueries__ region");
        }
      } else {
        logger.error("Cannot create ParameterizedQueries Region as no cache found!");
      }
    } catch (Exception e) {
      if (logger.isDebugEnabled()) {
        logger.debug("Error creating __ParameterizedQueries__ Region with cause {}",
            e.getMessage(), e);
      }
    }
  }
}
