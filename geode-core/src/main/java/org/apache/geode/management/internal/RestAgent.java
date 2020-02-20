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

import java.net.URI;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.internal.HttpService;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.internal.cache.CacheFactoryStatics;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegionFactory;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Agent implementation that controls the HTTP server end points used for REST clients to connect
 * gemfire data node.
 * <p>
 * The RestAgent is used to start http service in embedded mode on any non manager data node with
 * developer REST APIs service enabled.
 *
 * @since GemFire 8.0
 */
public class RestAgent {
  private static final Logger logger = LogService.getLogger();

  private boolean running = false;
  private final DistributionConfig config;
  private final SecurityService securityService;

  public RestAgent(DistributionConfig config, SecurityService securityService) {
    this.config = config;
    this.securityService = securityService;
  }

  public synchronized boolean isRunning() {
    return this.running;
  }


  public synchronized void start(InternalCache cache) {
    if (!this.running && this.config.getHttpServicePort() != 0) {
      try {
        startHttpService(cache);
        this.running = true;
        cache.setRESTServiceRunning(true);

        // create region to hold query information (queryId, queryString). Added
        // for the developer REST APIs
        RestAgent.createParameterizedQueryRegion();
      } catch (Throwable e) {
        logger.warn("Unable to start dev REST API: {}", e.toString());
      }
    }
  }

  private final String GEMFIRE_VERSION = GemFireVersion.getGemFireVersion();
  private AgentUtil agentUtil = new AgentUtil(GEMFIRE_VERSION);

  private boolean isRunningInTomcat() {
    return (System.getProperty("catalina.base") != null
        || System.getProperty("catalina.home") != null);
  }

  // Start HTTP service in embedded mode
  public void startHttpService(InternalCache cache) throws Exception {
    // Find the developer REST WAR file
    final URI gemfireAPIWar = agentUtil.findWarLocation("geode-web-api");
    if (gemfireAPIWar == null) {
      logger.info(
          "Unable to find GemFire Developer REST API WAR file; the Developer REST Interface for GemFire will not be accessible.");
    }

    // Check if we're already running inside Tomcat
    if (isRunningInTomcat()) {
      logger.warn(
          "Detected presence of catalina system properties. HTTP service will not be started. To enable the GemFire Developer REST API, please deploy the /geode-web-api WAR file in your application server.");
    } else if (agentUtil.isAnyWarFileAvailable(gemfireAPIWar)) {

      Map<String, Object> securityServiceAttr = new HashMap<>();
      securityServiceAttr.put(HttpService.SECURITY_SERVICE_SERVLET_CONTEXT_PARAM,
          securityService);

      if (cache.getOptionalService(HttpService.class).isPresent()) {
        HttpService httpService = cache.getOptionalService(HttpService.class).get();
        Path gemfireAPIWarPath = Paths.get(gemfireAPIWar);
        httpService.addWebApplication("/gemfire-api", gemfireAPIWarPath, securityServiceAttr);
        httpService.addWebApplication("/geode", gemfireAPIWarPath, securityServiceAttr);
      } else {
        logger.warn("HttpService is not available - could not start Dev REST API");
      }
    }
  }

  public static String getBindAddressForHttpService(DistributionConfig config) {
    String bindAddress = config.getHttpServiceBindAddress();
    if (StringUtils.isNotBlank(bindAddress))
      return bindAddress;

    bindAddress = config.getServerBindAddress();
    if (StringUtils.isNotBlank(bindAddress))
      return bindAddress;

    bindAddress = config.getBindAddress();
    if (StringUtils.isNotBlank(bindAddress))
      return bindAddress;

    try {
      bindAddress = LocalHostUtil.getLocalHost().getHostAddress();
      logger.info("RestAgent.getBindAddressForHttpService.localhost: "
          + LocalHostUtil.getLocalHost().getHostAddress());
    } catch (UnknownHostException e) {
      logger.error("LocalHost could not be found.", e);
    }
    return bindAddress;
  }

  /**
   * This method will create a REPLICATED region named _ParameterizedQueries__. In developer REST
   * APIs, this region will be used to store the queryId and queryString as a key and value
   * respectively.
   */
  public static void createParameterizedQueryRegion() {
    try {
      if (logger.isDebugEnabled()) {
        logger.debug("Starting creation of  __ParameterizedQueries__ region");
      }
      InternalCache cache = CacheFactoryStatics.getAnyInstance();
      if (cache != null) {
        InternalRegionFactory<String, String> factory =
            cache.createInternalRegionFactory(RegionShortcut.REPLICATE);
        factory.setConcurrencyChecksEnabled(false);
        factory.setKeyConstraint(String.class);
        factory.setStatisticsEnabled(false);
        factory.setValueConstraint(String.class);
        factory.setIsUsedForMetaRegion(true);
        factory.create("__ParameterizedQueries__");
        if (logger.isDebugEnabled()) {
          logger.debug("Successfully created __ParameterizedQueries__ region");
        }
      } else {
        logger.error("Cannot create ParameterizedQueries Region as no cache found!");
      }
    } catch (Exception e) {
      if (logger.isDebugEnabled()) {
        logger.debug("Error creating __ParameterizedQueries__ Region with cause {}", e.getMessage(),
            e);
      }
    }
  }
}
