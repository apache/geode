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
package org.apache.geode.modules.session.bootstrap;

import static org.apache.geode.distributed.ConfigurationProperties.CACHE_XML_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_PREFIX;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_ARCHIVE_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_SAMPLING_ENABLED;

import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.geode.cache.GemFireCache;
import org.apache.geode.cache.control.ResourceManager;
import org.apache.geode.distributed.internal.AbstractDistributionConfig;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.modules.util.Banner;
import org.apache.geode.modules.util.RegionHelper;
import org.apache.geode.modules.util.ResourceManagerValidator;
import org.apache.geode.util.internal.GeodeGlossary;

public abstract class AbstractCache {

  protected GemFireCache cache;

  private static final DateFormat FORMAT = new SimpleDateFormat("yyyy-MM-dd");

  private static final String DEFAULT_LOG_FILE_NAME =
      RegionHelper.NAME + "." + FORMAT.format(new Date()) + ".log";

  private static final String DEFAULT_STATISTIC_ARCHIVE_FILE_NAME = RegionHelper.NAME + ".gfs";

  private static final float DEFAULT_EVICTION_HEAP_PERCENTAGE =
      LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE;

  private static final float DEFAULT_CRITICAL_HEAP_PERCENTAGE =
      ResourceManager.DEFAULT_CRITICAL_PERCENTAGE;

  private static final String GEMFIRE_PREFIX = GeodeGlossary.GEMFIRE_PREFIX;
  private static final String DEFAULT_CACHE_XML_FILE =
      DistributionConfig.DEFAULT_CACHE_XML_FILE.getName();

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractCache.class);

  private float evictionHeapPercentage = DEFAULT_EVICTION_HEAP_PERCENTAGE;

  private float criticalHeapPercentage = DEFAULT_CRITICAL_HEAP_PERCENTAGE;

  private boolean rebalance = false;

  private final Map<String, String> gemfireProperties;

  private final AtomicBoolean started = new AtomicBoolean(false);

  /**
   * Instance reference which is set in static initialization blocks of any subclasses.
   */
  protected static AbstractCache instance = null;

  AbstractCache() {
    gemfireProperties = new ConcurrentHashMap<>();
  }

  public void lifecycleEvent(LifecycleTypeAdapter eventType) {
    if (getLogger().isDebugEnabled()) {
      getLogger().debug("Received " + eventType + " event");
    }

    if (eventType.equals(LifecycleTypeAdapter.START) && started.compareAndSet(false, true)) {
      // Create or retrieve the cache
      getLogger().info("Initializing " + Banner.getString());
      createOrRetrieveCache();

      // Initialize the resource manager
      initializeResourceManager();
    } else if (eventType.equals(LifecycleTypeAdapter.AFTER_START)) {
      if (getRebalance()) {
        rebalanceCache();
      }
    } else if (eventType.equals(LifecycleTypeAdapter.STOP)) {
      started.set(false);
    }
  }

  public boolean isStarted() {
    return started.get();
  }

  public void close() {
    getCache().close();
    while (!getCache().isClosed()) {
    }


    started.set(false);
  }

  public GemFireCache getCache() {
    return cache;
  }

  private String getLogFileName() {
    String logFileName = getGemFireProperties().get(LOG_FILE);
    if (logFileName == null) {
      logFileName = DEFAULT_LOG_FILE_NAME;
    }
    return logFileName;
  }

  private String getStatisticArchiveFileName() {
    String statisticsArchiveFileName = getGemFireProperties().get(STATISTIC_ARCHIVE_FILE);
    if (statisticsArchiveFileName == null) {
      statisticsArchiveFileName = DEFAULT_STATISTIC_ARCHIVE_FILE_NAME;
    }
    return statisticsArchiveFileName;
  }

  private String getCacheXmlFileName() {
    String cacheXmlFileName = getGemFireProperties().get(CACHE_XML_FILE);
    if (cacheXmlFileName == null) {
      cacheXmlFileName = getDefaultCacheXmlFileName();
    }
    return cacheXmlFileName;
  }

  private File getCacheXmlFile() {
    String cacheXmlFileName = getCacheXmlFileName();
    File cacheXmlFile = new File(cacheXmlFileName);
    // If the cache xml file is not absolute, point it at the conf directory.
    if (!cacheXmlFile.isAbsolute()) {
      if (System.getProperty("catalina.base") != null) {
        cacheXmlFile = new File(System.getProperty("catalina.base") + "/conf/", cacheXmlFileName);
      }
    }
    return cacheXmlFile;
  }

  public float getEvictionHeapPercentage() {
    return evictionHeapPercentage;
  }

  public void setEvictionHeapPercentage(String evictionHeapPercentage) {
    this.evictionHeapPercentage = Float.parseFloat(evictionHeapPercentage);
  }

  public float getCriticalHeapPercentage() {
    return criticalHeapPercentage;
  }

  public void setCriticalHeapPercentage(String criticalHeapPercentage) {
    this.criticalHeapPercentage = Float.parseFloat(criticalHeapPercentage);
  }

  public void setRebalance(boolean rebalance) {
    this.rebalance = rebalance;
  }

  public boolean getRebalance() {
    return rebalance;
  }

  private Map<String, String> getGemFireProperties() {
    return gemfireProperties;
  }

  public void setProperty(String name, String value) {
    // TODO Look at fake attributes
    if (name.equals("className")) {
      return;
    }

    // Determine the validity of the input property (all those that start with security-* are valid)
    boolean validProperty = name.startsWith(SECURITY_PREFIX);
    if (!validProperty) {
      // TODO: AbstractDistributionConfig is internal and _getAttNames is designed for testing.
      for (String gemfireProperty : AbstractDistributionConfig._getAttNames()) {
        if (name.equals(gemfireProperty)) {
          validProperty = true;
          break;
        }
      }
    }

    // If it is a valid GemFire property, add it to the GemFire properties, log a warning otherwise.
    if (validProperty) {
      gemfireProperties.put(name, value);
    } else {
      getLogger().warn("The input property named " + name
          + " is not a valid GemFire property. It is being ignored.");
    }
  }

  public Logger getLogger() {
    return LOGGER;
  }

  Properties createDistributedSystemProperties() {
    Properties properties = new Properties();

    // Add any additional gemfire properties
    properties.putAll(gemfireProperties);

    // Replace the cache xml file in the properties
    File cacheXmlFile = getCacheXmlFile();
    String absoluteCacheXmlFileName = cacheXmlFile.getAbsolutePath();
    // If the file doesn't exist and the name is the default, set cache-xml-file
    // to the GemFire default. This is for the case where only the jars have been
    // installed and no default cache.xml exists in the conf directory.
    if (getCacheXmlFileName().equals(getDefaultCacheXmlFileName()) && !cacheXmlFile.exists()) {
      absoluteCacheXmlFileName = DEFAULT_CACHE_XML_FILE;
    }
    properties.put(CACHE_XML_FILE, absoluteCacheXmlFileName);

    // Replace the log file in the properties
    properties.put(LOG_FILE, getLogFile().getAbsolutePath());

    // Replace the statistics archive file in the properties
    File statisticArchiveFile = getStatisticArchiveFile();
    if (statisticArchiveFile == null) {
      // Remove the statistics archive file name since statistic sampling is disabled
      properties.remove(STATISTIC_ARCHIVE_FILE);
      properties.remove(STATISTIC_SAMPLING_ENABLED);
    } else {
      properties.put(STATISTIC_ARCHIVE_FILE, statisticArchiveFile.getAbsolutePath());
    }
    getLogger().info("Creating distributed system from: " + properties);

    return properties;
  }

  private File getLogFile() {
    String logFileName = getLogFileName();
    File logFile = new File(logFileName);
    // If the log file is not absolute, point it at the logs directory.
    if (!logFile.isAbsolute()) {
      if (System.getProperty("catalina.base") != null) {
        logFile = new File(System.getProperty("catalina.base") + "/logs/", logFileName);
      } else if (System.getProperty("weblogic.Name") != null) {
        String weblogicName = System.getProperty("weblogic.Name");
        String separator = System.getProperty("file.separator");
        logFile = new File(
            "servers" + separator + weblogicName + separator + "logs" + separator + logFileName);
      } else {
        logFile =
            new File(System.getProperty(GEMFIRE_PREFIX + "logdir"), logFileName);
      }
    }
    return logFile;
  }

  private File getStatisticArchiveFile() {
    File statisticsArchiveFile = null;
    String statisticSamplingEnabled = getGemFireProperties().get(STATISTIC_SAMPLING_ENABLED);
    if (statisticSamplingEnabled != null && statisticSamplingEnabled.equals("true")) {
      String statisticsArchiveFileName = getStatisticArchiveFileName();
      statisticsArchiveFile = new File(statisticsArchiveFileName);
      // If the statistics archive file is not absolute, point it at the logs directory.
      if (!statisticsArchiveFile.isAbsolute()) {
        if (System.getProperty("catalina.base") != null) {
          statisticsArchiveFile =
              new File(System.getProperty("catalina.base") + "/logs/", statisticsArchiveFileName);
        } else if (System.getProperty("weblogic.Name") != null) {
          String weblogicName = System.getProperty("weblogic.Name");
          String separator = System.getProperty("file.separator");
          statisticsArchiveFile = new File("servers" + separator + weblogicName + separator + "logs"
              + separator + statisticsArchiveFileName);
        } else {
          statisticsArchiveFile =
              new File(System.getProperty(GEMFIRE_PREFIX + "statisticsdir"),
                  statisticsArchiveFileName);
        }
      }
    }
    return statisticsArchiveFile;
  }

  private void initializeResourceManager() {
    // Get current eviction and critical heap percentages
    ResourceManager rm = getCache().getResourceManager();
    float currentEvictionHeapPercentage = rm.getEvictionHeapPercentage();
    float currentCriticalHeapPercentage = rm.getCriticalHeapPercentage();

    // Set new eviction and critical heap percentages if necessary
    if (getEvictionHeapPercentage() != currentEvictionHeapPercentage
        || getCriticalHeapPercentage() != currentCriticalHeapPercentage) {
      if (getLogger().isDebugEnabled()) {
        StringBuilder builder = new StringBuilder();
        builder.append("Previous eviction heap percentage=").append(currentEvictionHeapPercentage)
            .append("; critical heap percentage=").append(currentCriticalHeapPercentage);
        getLogger().debug(builder.toString());
        builder.setLength(0);
        builder.append("Requested eviction heap percentage=").append(getEvictionHeapPercentage())
            .append("; critical heap percentage=").append(getCriticalHeapPercentage());
        getLogger().debug(builder.toString());
      }
      if (currentCriticalHeapPercentage == 0.0f) {
        // If the current critical heap percentage is 0 (disabled), set eviction
        // heap percentage first, then set the critical heap percentage. At this
        // point, the eviction heap percentage can be set to anything.
        try {
          rm.setEvictionHeapPercentage(getEvictionHeapPercentage());
          rm.setCriticalHeapPercentage(getCriticalHeapPercentage());
        } catch (IllegalArgumentException e) {
          handleResourceManagerException(e, currentEvictionHeapPercentage,
              currentCriticalHeapPercentage);
          rm.setEvictionHeapPercentage(currentEvictionHeapPercentage);
          rm.setCriticalHeapPercentage(currentCriticalHeapPercentage);
        }
      } else if (getCriticalHeapPercentage() >= currentCriticalHeapPercentage) {
        // If the requested critical heap percentage is >= the current critical
        // heap percentage, then set the critical heap percentage first since it
        // can safely be slid up. Then, set the eviction heap percentage.
        try {
          rm.setCriticalHeapPercentage(getCriticalHeapPercentage());
          rm.setEvictionHeapPercentage(getEvictionHeapPercentage());
        } catch (IllegalArgumentException e) {
          handleResourceManagerException(e, currentEvictionHeapPercentage,
              currentCriticalHeapPercentage);
          rm.setCriticalHeapPercentage(currentCriticalHeapPercentage);
          rm.setEvictionHeapPercentage(currentEvictionHeapPercentage);
        }
      } else {
        // If the requested critical heap percentage is < the current critical
        // heap percentage, then set the eviction heap percentage first since it
        // can safely be slid down. Then, set the critical heap percentage.
        try {
          rm.setEvictionHeapPercentage(getEvictionHeapPercentage());
          rm.setCriticalHeapPercentage(getCriticalHeapPercentage());
        } catch (IllegalArgumentException e) {
          handleResourceManagerException(e, currentEvictionHeapPercentage,
              currentCriticalHeapPercentage);
          rm.setEvictionHeapPercentage(currentEvictionHeapPercentage);
          rm.setCriticalHeapPercentage(currentCriticalHeapPercentage);
        }
      }
      if (getLogger().isDebugEnabled()) {
        String builder = "Actual eviction heap percentage=" + rm.getEvictionHeapPercentage()
            + "; critical heap percentage=" + rm.getCriticalHeapPercentage();
        getLogger().debug(builder);
      }
    }

    // Validate java startup parameters (done after setting the eviction and
    // critical heap percentages so that the CMSInitiatingOccupancyFraction can
    // be compared against them.
    ResourceManagerValidator.validateJavaStartupParameters(getCache());
  }

  private void handleResourceManagerException(IllegalArgumentException e,
      float currentEvictionHeapPercentage, float currentCriticalHeapPercentage) {
    String builder = "Caught exception attempting to set eviction heap percentage="
        + getEvictionHeapPercentage() + " and critical heap percentage="
        + getCriticalHeapPercentage()
        + ". The percentages will be set back to their previous values (eviction heap percentage="
        + currentEvictionHeapPercentage + " and critical heap percentage="
        + currentCriticalHeapPercentage + ").";
    getLogger().warn(builder, e);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "[" + "cache="
        + cache + "]";
  }

  protected abstract void createOrRetrieveCache();

  protected abstract void rebalanceCache();

  protected abstract String getDefaultCacheXmlFileName();
}
