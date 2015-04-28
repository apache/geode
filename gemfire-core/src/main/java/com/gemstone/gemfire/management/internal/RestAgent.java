/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.management.internal;

import java.io.File;

import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.springframework.util.Assert;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.GemFireVersion;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.InternalRegionArguments;
import com.gemstone.gemfire.internal.lang.StringUtils;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.management.ManagementService;

/**
 * Agent implementation that controls the HTTP server end points used for REST 
 * clients to connect gemfire data node.
 * 
 * The RestAgent is used to start http service in embedded mode on any non manager data node with developer REST APIs service enabled. 
 *
 * @author Nilkanth Patel.
 * @since 8.0
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
  
  private boolean isManagementRestServiceRunning(GemFireCacheImpl cache){
    final SystemManagementService managementService = (SystemManagementService) ManagementService.getManagementService(
        cache);
    return ( managementService.getManagementAgent() != null && managementService.getManagementAgent().isHttpServiceRunning());
    
  }
  
  public synchronized void start(GemFireCacheImpl cache){
    if (!this.running 
        && this.config.getHttpServicePort() != 0 
        && !isManagementRestServiceRunning(cache)) {
      try {
        startHttpService();
        this.running = true;
        cache.setRESTServiceRunning(true);
        
        //create region to hold query information (queryId, queryString). Added for the developer REST APIs
        RestAgent.createParameterizedQueryRegion();
        
      } catch (RuntimeException e){
        logger.debug(e.getMessage(), e);
      }
    }
      
  }
  
  public synchronized void stop(){
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
  
  //Start HTTP service in embedded mode
  public void startHttpService() {
    //TODO: add a check that will make sure that we start HTTP service on non-manager data node
    logger.info("Attempting to start HTTP service on port ({}) at bind-address ({})...",
        this.config.getHttpServicePort(), this.config.getHttpServiceBindAddress());
      
    String gemfireHome = System.getenv("GEMFIRE");
    
    // Check for empty variable. if empty, then log message and exit HTTP server startup
    if (StringUtils.isBlank(gemfireHome)) {
      String gemfire = System.getProperty("gemfire.home");
      logger.info("Reading gemfire.home System Property -> {}", gemfire);
      if (StringUtils.isBlank(gemfire)) {
        logger.info("GEMFIRE environment variable not set; HTTP service will not start.");
        return;
      } else {
        gemfireHome = gemfire;
      }
    }
      
    // Find the developer REST WAR file
    final String gemfireAPIWar =  getGemFireAPIWarLocation(gemfireHome);
      
    if(gemfireAPIWar == null){
      logger.info("Unable to find GemFire Developer REST API WAR file; the Developer REST API for GemFire will not be exported and accessible.");
    }
      
    try {
      // Check if we're already running inside Tomcat
      if (isRunningInTomcat()) {
        logger.warn("Detected presence of catalina system properties. HTTP service will not be started. To enable the GemFire developer REST API, please deploy the gemfire-web.war file in your application server."); 
      }
      else if (isWebApplicationAvailable(gemfireAPIWar)) {
          
        final String bindAddress = this.config.getHttpServiceBindAddress();
        final int port = this.config.getHttpServicePort();

        this.httpServer = JettyHelper.initJetty(bindAddress, port,
            this.config.getHttpServiceSSLEnabled(),
            this.config.getHttpServiceSSLRequireAuthentication(), 
            this.config.getHttpServiceSSLProtocols(),
            this.config.getHttpServiceSSLCiphers(), 
            this.config.getHttpServiceSSLProperties()
            );
          
        this.httpServer = JettyHelper.addWebApplication(httpServer, "/gemfire-api", gemfireAPIWar);
        
        if (logger.isDebugEnabled()) {
          logger.debug("Starting HTTP embedded server on port ({}) at bind-address ({})...",
              ((ServerConnector)this.httpServer.getConnectors()[0]).getPort(), bindAddress);
        }

        this.httpServer = JettyHelper.startJetty(this.httpServer);
        logger.info("HTTP service started successfully...!!");
      }
    }catch (Exception e) {
      stopHttpService();//Jetty needs to be stopped even if it has failed to start. Some of the threads are left behind even if server.start() fails due to an exception
      throw new RuntimeException("HTTP service failed to start due to "+e.getMessage());
    }
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
   * This method will create a REPLICATED region named _ParameterizedQueries__.
   * In developer REST APIs, this region will be used to store the queryId and queryString as a key and value respectively.
   */
  public static void createParameterizedQueryRegion(){
    try {
      if (logger.isDebugEnabled()) {
        logger.debug("Starting creation of  __ParameterizedQueries__ region");
      }
      GemFireCacheImpl cache = (GemFireCacheImpl)CacheFactory.getAnyInstance();
      if (cache != null) {
        //cache.getCacheConfig().setPdxReadSerialized(true);
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
      }else {
        logger.error("Cannot create ParameterizedQueries Region as no cache found!");
      }
    }
    catch (Exception e) {
      if (logger.isDebugEnabled()) {
        logger.debug("Error creating __ParameterizedQueries__ Region with cause {}",e.getMessage(), e);
      }
    }
  }
}
