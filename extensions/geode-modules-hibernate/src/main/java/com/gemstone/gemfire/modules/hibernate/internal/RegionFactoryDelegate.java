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
package com.gemstone.gemfire.modules.hibernate.internal;

import java.util.Iterator;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.GemFireCache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.modules.hibernate.GemFireCacheProvider;
import com.gemstone.gemfire.modules.util.BootstrappingFunction;
import com.gemstone.gemfire.modules.util.CreateRegionFunction;
import com.gemstone.gemfire.modules.util.RegionConfiguration;

public class RegionFactoryDelegate  {

  private static final String LOG_FILE = "log-file";

  private static final String CACHE_XML_FILE = "cache-xml-file";

  private static final String DEFAULT_REGION_TYPE = RegionShortcut.REPLICATE_HEAP_LRU.name();

  private static final String CLIENT_DEFAULT_REGION_TYPE = ClientRegionShortcut.PROXY.name();

  protected final Properties gemfireProperties;
  protected final Properties regionProperties;

  protected Logger log = LoggerFactory.getLogger(getClass());
  
  private Cache cache;
  
  public RegionFactoryDelegate(Properties gemfireProperties, Properties regionProperties) {
    this.gemfireProperties = gemfireProperties;
    this.regionProperties = regionProperties;
  }
  
  public GemFireCache startCache() {
    log.info("Creating a GemFire cache");
    checkExistingCache();
    cache = new CacheFactory(gemfireProperties).create();
    log.debug("GemFire cache creation completed");
    FunctionService.onMembers(this.cache.getDistributedSystem()).execute(new BootstrappingFunction()).getResult();
    FunctionService.registerFunction(new CreateRegionFunction(cache));
    return cache;
  }

  /**
   * When hibernate module is running within servlet container, we should
   * check if http module is being used and make sure that we use 
   * same cache-xml and log-file properties.
   */
  protected void checkExistingCache() {
    Cache existingCache = GemFireCacheImpl.getInstance();
    if (existingCache == null) {
      return;
    }
    Properties existingProps = existingCache.getDistributedSystem().getProperties();
    String cacheXML = existingProps.getProperty(CACHE_XML_FILE);
    String logFile = existingProps.getProperty(LOG_FILE, "");
    this.gemfireProperties.setProperty(CACHE_XML_FILE, cacheXML);
    this.gemfireProperties.setProperty(LOG_FILE, logFile);
    log.info("Existing GemFire cache detected. Using same "+CACHE_XML_FILE+":"+cacheXML+
    " and "+LOG_FILE+":"+logFile+" as existing cache");
  }
  
  public Region<Object, EntityWrapper> createRegion(String regionName) {
    Region<Object, EntityWrapper> r = cache.getRegion(regionName);
    if (r != null) {
      // for  the peer-to-peer case, for now we assume that
      // cache.xml will be the same for all peers
      // TODO validate regions without this assumption
      return r;
    }
    String regionType = getRegionType(regionName);
    boolean isLocalRegion = regionType.contains("LOCAL") ? true : false;
    RegionConfiguration regionConfig = new RegionConfiguration();
    regionConfig.setRegionName(regionName);
    regionConfig.setRegionAttributesId(regionType);
    regionConfig.setCacheWriterName(EntityRegionWriter.class.getCanonicalName());
    com.gemstone.gemfire.cache.RegionFactory<Object, EntityWrapper> rFactory = this.cache
        .createRegionFactory(RegionShortcut.valueOf(regionType));
    rFactory.setCacheWriter(new EntityRegionWriter());
    if (isLocalRegion) {
      rFactory.setDataPolicy(DataPolicy.REPLICATE);
    }
    r = rFactory.create(regionName);
    // create same region on peers
    if (!isLocalRegion) {
      FunctionService.onMembers(this.cache.getDistributedSystem())
          .withArgs(regionConfig).execute(CreateRegionFunction.ID).getResult();
    }
    return r;
  }

  /**
   * returns the type of region to create by consulting the properties specified
   * in hibernate.cfg.xml
   * 
   * @see #createRegion(String)
   * @param regionName
   * @return string representation of {@link RegionShortcut}
   * @see GemFireCacheProvider
   */
  protected String getRegionType(String regionName) {
    String rType = getOverridenRegionType(regionName);
    if (rType != null) {
      return rType.toUpperCase();
    }
    rType = regionProperties
        .getProperty("gemfire.default-region-attributes-id");
    if (rType == null) {
      rType =  DEFAULT_REGION_TYPE;
    }
    return rType.toUpperCase();
  }

  private String getOverridenRegionType(String regionName) {
    String rType = null;
    Iterator<Object> it = regionProperties.keySet().iterator();
    while (it.hasNext()) {
      String current = (String)it.next();
      if (current.contains(regionName)) {
        rType = regionProperties.getProperty(current);
        break;
      }
    }
    return rType;
  }
}
