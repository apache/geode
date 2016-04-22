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
package com.gemstone.gemfire.modules.session.bootstrap;

import com.gemstone.gemfire.cache.GemFireCache;
import com.gemstone.gemfire.cache.control.ResourceManager;
import com.gemstone.gemfire.distributed.internal.AbstractDistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.modules.util.Banner;
import com.gemstone.gemfire.modules.util.RegionHelper;
import com.gemstone.gemfire.modules.util.ResourceManagerValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractCache {

  protected GemFireCache cache;

  private static final DateFormat FORMAT = new SimpleDateFormat("yyyy-MM-dd");

  protected static final String DEFAULT_LOG_FILE_NAME = RegionHelper.NAME + "." + FORMAT.format(new Date()) + ".log";

  protected static final String DEFAULT_STATISTIC_ARCHIVE_FILE_NAME = RegionHelper.NAME + ".gfs";

  protected static final float DEFAULT_EVICTION_HEAP_PERCENTAGE = LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE;

  protected static final float DEFAULT_CRITICAL_HEAP_PERCENTAGE = ResourceManager.DEFAULT_CRITICAL_PERCENTAGE;

  protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractCache.class);

  protected float evictionHeapPercentage = DEFAULT_EVICTION_HEAP_PERCENTAGE;

  protected float criticalHeapPercentage = DEFAULT_CRITICAL_HEAP_PERCENTAGE;

  protected boolean rebalance = false;

  protected final Map<String, String> gemfireProperties;

  private final AtomicBoolean started = new AtomicBoolean(false);

  /**
   * Instance reference which is set in static initialization blocks of any subclasses.
   */
  protected static AbstractCache instance = null;

  public AbstractCache() {
    this.gemfireProperties = new ConcurrentHashMap<String, String>();
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
      // Close the cache
//      closeCache();
      // TODO: Do we need to reset the started flag here?
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
    return this.cache;
  }

  public String getLogFileName() {
    String logFileName = getGemFireProperties().get(DistributionConfig.LOG_FILE_NAME);
    if (logFileName == null) {
      logFileName = DEFAULT_LOG_FILE_NAME;
    }
    return logFileName;
  }

  public String getStatisticArchiveFileName() {
    String statisticsArchiveFileName = getGemFireProperties().get(DistributionConfig.STATISTIC_ARCHIVE_FILE_NAME);
    if (statisticsArchiveFileName == null) {
      statisticsArchiveFileName = DEFAULT_STATISTIC_ARCHIVE_FILE_NAME;
    }
    return statisticsArchiveFileName;
  }

  public String getCacheXmlFileName() {
    String cacheXmlFileName = getGemFireProperties().get(DistributionConfig.CACHE_XML_FILE_NAME);
    if (cacheXmlFileName == null) {
      cacheXmlFileName = getDefaultCacheXmlFileName();
    }
    return cacheXmlFileName;
  }

  protected File getCacheXmlFile() {
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
    return this.evictionHeapPercentage;
  }

  public void setEvictionHeapPercentage(String evictionHeapPercentage) {
    this.evictionHeapPercentage = Float.valueOf(evictionHeapPercentage);
  }

  public float getCriticalHeapPercentage() {
    return this.criticalHeapPercentage;
  }

  public void setCriticalHeapPercentage(String criticalHeapPercentage) {
    this.criticalHeapPercentage = Float.valueOf(criticalHeapPercentage);
  }

  public void setRebalance(boolean rebalance) {
    this.rebalance = rebalance;
  }

  public boolean getRebalance() {
    return this.rebalance;
  }

  public Map<String, String> getGemFireProperties() {
    return this.gemfireProperties;
  }

  public void setProperty(String name, String value) {
    //TODO Look at fake attributes
    if (name.equals("className")) {
      return;
    }

    // Determine the validity of the input property
    boolean validProperty = false;
    for (String gemfireProperty : AbstractDistributionConfig._getAttNames()) {
      if (name.equals(gemfireProperty)) {
        validProperty = true;
        break;
      }
    }

    // If it is a valid GemFire property, add it to the the GemFire properties.
    // Otherwise, log a warning.
    if (validProperty) {
      this.gemfireProperties.put(name, value);
    } else {
      getLogger().warn("The input property named " + name + " is not a valid GemFire property. It is being ignored.");
    }
  }

  public Logger getLogger() {
    return LOGGER;
  }

  protected Properties createDistributedSystemProperties() {
    Properties properties = new Properties();

    // Add any additional gemfire properties
    for (Map.Entry<String, String> entry : this.gemfireProperties.entrySet()) {
      properties.put(entry.getKey(), entry.getValue());
    }

    // Replace the cache xml file in the properties
    File cacheXmlFile = getCacheXmlFile();
    String absoluteCacheXmlFileName = cacheXmlFile.getAbsolutePath();
    // If the file doesn't exist and the name is the default, set cache-xml-file
    // to the GemFire default. This is for the case where only the jars have been
    // installed and no default cache.xml exists in the conf directory.
    if (getCacheXmlFileName().equals(getDefaultCacheXmlFileName()) && !cacheXmlFile.exists()) {
      absoluteCacheXmlFileName = DistributionConfig.DEFAULT_CACHE_XML_FILE.getName();
    }
    properties.put(DistributionConfig.CACHE_XML_FILE_NAME, absoluteCacheXmlFileName);

    // Replace the log file in the properties
    properties.put(DistributionConfig.LOG_FILE_NAME, getLogFile().getAbsolutePath());

    // Replace the statistics archive file in the properties
    File statisticArchiveFile = getStatisticArchiveFile();
    if (statisticArchiveFile == null) {
      // Remove the statistics archive file name since statistic sampling is disabled
      properties.remove(DistributionConfig.STATISTIC_ARCHIVE_FILE_NAME);
      properties.remove(DistributionConfig.STATISTIC_SAMPLING_ENABLED_NAME);
    } else {
      properties.put(DistributionConfig.STATISTIC_ARCHIVE_FILE_NAME, statisticArchiveFile.getAbsolutePath());
    }
    getLogger().info("Creating distributed system from: " + properties);

    return properties;
  }

  protected void closeCache() {
    if (getLogger().isDebugEnabled()) {
      getLogger().debug("Closing " + this.cache);
    }
    if (getCache() != null) {
      getCache().close();
    }
    getLogger().info("Closed " + this.cache);
  }

  protected File getLogFile() {
    String logFileName = getLogFileName();
    File logFile = new File(logFileName);
    // If the log file is not absolute, point it at the logs directory.
    if (!logFile.isAbsolute()) {
      if (System.getProperty("catalina.base") != null) {
        logFile = new File(System.getProperty("catalina.base") + "/logs/", logFileName);
      } else if (System.getProperty("weblogic.Name") != null) {
        String weblogicName = System.getProperty("weblogic.Name");
        String separator = System.getProperty("file.separator");
        logFile = new File("servers" + separator + weblogicName + separator +
            "logs" + separator + logFileName);
      } else {
        logFile = new File(System.getProperty("gemfire.logdir"), logFileName);
      }
    }
    return logFile;
  }

  protected File getStatisticArchiveFile() {
    File statisticsArchiveFile = null;
    String statisticSamplingEnabled = getGemFireProperties().get(DistributionConfig.STATISTIC_SAMPLING_ENABLED_NAME);
    if (statisticSamplingEnabled != null && statisticSamplingEnabled.equals("true")) {
      String statisticsArchiveFileName = getStatisticArchiveFileName();
      statisticsArchiveFile = new File(statisticsArchiveFileName);
      // If the statistics archive file is not absolute, point it at the logs directory.
      if (!statisticsArchiveFile.isAbsolute()) {
        if (System.getProperty("catalina.base") != null) {
          statisticsArchiveFile = new File(System.getProperty("catalina.base") + "/logs/", statisticsArchiveFileName);
        } else if (System.getProperty("weblogic.Name") != null) {
          String weblogicName = System.getProperty("weblogic.Name");
          String separator = System.getProperty("file.separator");
          statisticsArchiveFile = new File("servers" + separator + weblogicName + separator +
              "logs" + separator + statisticsArchiveFileName);
        } else {
          statisticsArchiveFile = new File(System.getProperty("gemfire.statisticsdir"), statisticsArchiveFileName);
        }
      }
    }
    return statisticsArchiveFile;
  }

  protected void initializeResourceManager() {
    // Get current eviction and critical heap percentages
    ResourceManager rm = getCache().getResourceManager();
    float currentEvictionHeapPercentage = rm.getEvictionHeapPercentage();
    float currentCriticalHeapPercentage = rm.getCriticalHeapPercentage();

    // Set new eviction and critical heap percentages if necessary
    if (getEvictionHeapPercentage() != currentEvictionHeapPercentage || getCriticalHeapPercentage() != currentCriticalHeapPercentage) {
      if (getLogger().isDebugEnabled()) {
        StringBuilder builder = new StringBuilder();
        builder.append("Previous eviction heap percentage=")
            .append(currentEvictionHeapPercentage)
            .append("; critical heap percentage=")
            .append(currentCriticalHeapPercentage);
        getLogger().debug(builder.toString());
        builder.setLength(0);
        builder.append("Requested eviction heap percentage=")
            .append(getEvictionHeapPercentage())
            .append("; critical heap percentage=")
            .append(getCriticalHeapPercentage());
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
          handleResourceManagerException(e, currentEvictionHeapPercentage, currentCriticalHeapPercentage);
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
          handleResourceManagerException(e, currentEvictionHeapPercentage, currentCriticalHeapPercentage);
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
          handleResourceManagerException(e, currentEvictionHeapPercentage, currentCriticalHeapPercentage);
          rm.setEvictionHeapPercentage(currentEvictionHeapPercentage);
          rm.setCriticalHeapPercentage(currentCriticalHeapPercentage);
        }
      }
      if (getLogger().isDebugEnabled()) {
        StringBuilder builder = new StringBuilder();
        builder.append("Actual eviction heap percentage=")
            .append(rm.getEvictionHeapPercentage())
            .append("; critical heap percentage=")
            .append(rm.getCriticalHeapPercentage());
        getLogger().debug(builder.toString());
      }
    }

    // Validate java startup parameters (done after setting the eviction and
    // critical heap percentages so that the CMSInitiatingOccupancyFraction can
    // be compared against them.
    ResourceManagerValidator.validateJavaStartupParameters(getCache());
  }

  private void handleResourceManagerException(IllegalArgumentException e, float currentEvictionHeapPercentage,
      float currentCriticalHeapPercentage) {
    StringBuilder builder = new StringBuilder();
    builder.append("Caught exception attempting to set eviction heap percentage=")
        .append(getEvictionHeapPercentage())
        .append(" and critical heap percentage=")
        .append(getCriticalHeapPercentage())
        .append(". The percentages will be set back to their previous values (eviction heap percentage=")
        .append(currentEvictionHeapPercentage)
        .append(" and critical heap percentage=")
        .append(currentCriticalHeapPercentage)
        .append(").");
    getLogger().warn(builder.toString(), e);
  }

  @Override
  public String toString() {
    return new StringBuilder().append(getClass().getSimpleName())
        .append("[")
        .append("cache=")
        .append(this.cache)
        .append("]")
        .toString();
  }

  protected abstract void createOrRetrieveCache();

  protected abstract void rebalanceCache();

  protected abstract String getDefaultCacheXmlFileName();
}
