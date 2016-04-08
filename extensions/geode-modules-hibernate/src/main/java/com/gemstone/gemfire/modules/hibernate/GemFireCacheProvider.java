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
package com.gemstone.gemfire.modules.hibernate;

import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.distributed.DistributedLockService;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.modules.util.CreateRegionFunction;
import com.gemstone.gemfire.modules.util.RegionConfiguration;
import org.apache.logging.log4j.Logger;
import org.hibernate.cache.Cache;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.CacheProvider;
import org.hibernate.cache.Timestamper;

import java.util.Iterator;
import java.util.Properties;

@SuppressWarnings("deprecation")
public class GemFireCacheProvider implements CacheProvider {

  private static final Logger logger = LogService.getLogger();

  protected com.gemstone.gemfire.cache.Cache _cache;

  private DistributedLockService distributedLockService;

  private Properties regionAttributes = new Properties();

  private final String DEFAULT_REGION_TYPE = RegionShortcut.REPLICATE_HEAP_LRU
      .name();

  private final String HIBERNATE_DLOCK_SERVICE_NAME = "hibernate-cache-lock-service";
  /**
   * Configure the cache
   * 
   * @param regionName
   *          the name of the cache region
   * @param properties
   *          configuration settings
   * @throws CacheException
   */
  public Cache buildCache(String regionName, Properties properties)
      throws CacheException {
    logger.info("GemFireCacheProvider: Creating cache: " + regionName);
    Region region = retrieveOrCreateRegion(regionName);
    Cache cache = null;
    if (region == null) {
      // Doh, blow up
      throw new RuntimeException("Couldn't find cache region : " + regionName);
    }
    else {
      cache = new GemFireCache(region, this.distributedLockService);
    }
    logger.info("GemFireCacheProvider: Created cache: " + regionName + "->" + cache);
    return cache;
  }

  public boolean isMinimalPutsEnabledByDefault() {
    return false;
  }

  /**
   * Generate a timestamp
   */
  public long nextTimestamp() {
    return Timestamper.next();
  }

  /**
   * Returns the region if already created, otherwise first tries to create it
   * from cache.xml, if not specified in cache.xml, create the region from the
   * properties specified in hibernate.cfg.xml. Two types of properties can be
   * specified in hibernate.cfg.xml
   * <ol>
   * <li>gemfire.default-region-attributes-id: the default region type to
   * create. (default value for this is REPLICATE)
   * <li>gemfire.region-attributes-for:fullyQualifiedRegionName when a region
   * wants to override the default region type
   * </ol>
   * 
   * @param regionName
   * @return the region
   */
  protected Region retrieveOrCreateRegion(String regionName) {
    // TODO client regions
    Region r = _cache.getRegion(regionName);
    if (r == null) {
      String regionType = getRegionType(regionName);
      r = _cache.createRegionFactory(RegionShortcut.valueOf(regionType))
          .create(regionName);
      RegionConfiguration regionConfig = new RegionConfiguration();
      regionConfig.setRegionName(regionName);
      regionConfig.setRegionAttributesId(regionType);
      FunctionService.onMembers(_cache.getDistributedSystem())
          .withArgs(regionConfig).execute(CreateRegionFunction.ID).getResult();
    }
    return r;
  }

  /**
   * returns the type of region to create by consulting the properties specified
   * in hibernate.cfg.xml
   * 
   * @see #retrieveOrCreateRegion(String)
   * @param regionName
   * @return string representation of {@link RegionShortcut}
   */
  private String getRegionType(String regionName) {
    String rType = regionAttributes
        .getProperty("gemfire.default-region-attributes-id");
    if (rType == null) {
      rType = DEFAULT_REGION_TYPE;
    }
    // iterate to find overridden property for a region
    Iterator<Object> it = regionAttributes.keySet().iterator();
    while (it.hasNext()) {
      String current = (String)it.next();
      if (current.contains(regionName)) {
        rType = regionAttributes.getProperty(current);
        break;
      }
    }
    return rType.toUpperCase();
  }

  /**
   * Callback to perform any necessary initialization of the underlying cache
   * implementation during SessionFactory construction.
   * 
   * @param properties
   *          current configuration settings.
   */
  public void start(Properties properties) throws CacheException {
    logger.info("GemFireCacheProvider: Creating cache provider");

    // We have to strip out any unknown properties, do so here
    Properties gemfireOnlyProperties = new Properties();
    for (Object keyObj : properties.keySet()) {
      String key = (String)keyObj;
      if (key.contains("region-attributes")) {
        regionAttributes.put(key, properties.get(key));
      }
      else if (key.startsWith("gemfire.")) {
        gemfireOnlyProperties.setProperty(key.replace("gemfire.", ""),
            properties.getProperty(key));
      }
    }

    // Create cache and d-lock service
    try {
      _cache = new CacheFactory(gemfireOnlyProperties).create();
      DistributedLockService existing = DistributedLockService.getServiceNamed(HIBERNATE_DLOCK_SERVICE_NAME);
      if (existing == null) {
        this.distributedLockService = DistributedLockService.create(
            HIBERNATE_DLOCK_SERVICE_NAME, _cache.getDistributedSystem());
      } else {
        this.distributedLockService = existing;
      }
    } 
    catch (com.gemstone.gemfire.cache.CacheException e) {
      throw new CacheException(e);
    }

    FunctionService.registerFunction(new CreateRegionFunction());

    logger.info("GemFireCacheProvider: Done creating cache provider");
  }

  /**
   * Callback to perform any necessary cleanup of the underlying cache
   * implementation during SessionFactory.close().
   */
  public void stop() {
    logger.info("GemFireCacheProvider: Stopping");
    _cache.close();
    logger.info("GemFireCacheProvider: Stopped");
  }

  public static Logger getLogger() {
    return logger;
  }
}
