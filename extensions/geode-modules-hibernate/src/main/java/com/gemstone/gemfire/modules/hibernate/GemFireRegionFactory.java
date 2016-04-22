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

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.hibernate.cache.CacheDataDescription;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.CollectionRegion;
import org.hibernate.cache.EntityRegion;
import org.hibernate.cache.QueryResultsRegion;
import org.hibernate.cache.RegionFactory;
import org.hibernate.cache.Timestamper;
import org.hibernate.cache.TimestampsRegion;
import org.hibernate.cache.access.AccessType;
import org.hibernate.cfg.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.GemFireCache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionConfigImpl;
import com.gemstone.gemfire.modules.hibernate.internal.ClientServerRegionFactoryDelegate;
import com.gemstone.gemfire.modules.hibernate.internal.EntityWrapper;
import com.gemstone.gemfire.modules.hibernate.internal.GemFireCollectionRegion;
import com.gemstone.gemfire.modules.hibernate.internal.GemFireEntityRegion;
import com.gemstone.gemfire.modules.hibernate.internal.GemFireQueryResultsRegion;
import com.gemstone.gemfire.modules.hibernate.internal.RegionFactoryDelegate;
import com.gemstone.gemfire.modules.util.Banner;

public class GemFireRegionFactory implements RegionFactory {

    
  private static final String GEMFIRE_QUERY_RESULTS_REGION_NAME = "gemfire.hibernateQueryResults";

  private static final String GEMFIRE_TIMESTAMPS_REGION_NAME = "gemfire.hibernateTimestamps";

  private GemFireCache _cache;

  private RegionFactoryDelegate delegate;

  // TODO get rid of this
  private boolean isClient;
  
  private final Logger log = LoggerFactory.getLogger(getClass());

  private final ExecutorService executorService = Executors.newSingleThreadExecutor();

  private Set<String> gemfireAttributes;
  
  /**
   * maps the entity to the region that stores it.
   */
  private ConcurrentMap<String, GemFireEntityRegion> entityRegionMap = new ConcurrentHashMap<String, GemFireEntityRegion>();
  
  public GemFireRegionFactory(Properties props) {
    log.debug("props:" + props);
  }

  public ExecutorService getExecutorService() {
    return this.executorService;
  }
  
  @Override
  public void start(Settings settings, Properties properties)
      throws CacheException {
    log.info("Initializing " + Banner.getString());
    extractGemFireProperties(properties);
    _cache = delegate.startCache();
  }

  private void extractGemFireProperties(Properties properties) {
    // We have to strip out any unknown properties, do so here
    Properties gemfireProperties = new Properties();
    Properties regionProperties = new Properties();
    for (Object keyObj : properties.keySet()) {
      String key = (String)keyObj;
      if (key.contains("region-attributes")) {
        regionProperties.put(key, properties.get(key));
      }
      else if (key.equals("gemfire.cache-topology")) {
        if (properties.getProperty(key).trim()
            .equalsIgnoreCase("client-server")) {
          isClient = true;
        }
      }
      else if (key.startsWith("gemfire.") && isGemFireAttribute(key)) {
        gemfireProperties.setProperty(key.replace("gemfire.", ""),
            properties.getProperty(key));
      }
    }
    if (isClient) {
      delegate = new ClientServerRegionFactoryDelegate(gemfireProperties, regionProperties);
    } else {
      delegate = new RegionFactoryDelegate(gemfireProperties, regionProperties);
    }
  }

  private boolean isGemFireAttribute(String key) {
    String gfKey = key.replace("gemfire.", "");
    Set<String> gemfireAttributes = getGemFireAttributesNames();
    return gemfireAttributes.contains(gfKey);
  }

  private Set<String> getGemFireAttributesNames() {
    if (this.gemfireAttributes == null) {
      //used only to get the list of all gemfire properties
      DistributionConfig dConfig = new DistributionConfigImpl(new Properties());
      String[] gemfireAttributeNames = dConfig.getAttributeNames();
      gemfireAttributes = new HashSet<String>();
      for (String attrName : gemfireAttributeNames) {
        gemfireAttributes.add(attrName);
      }
    }
    return gemfireAttributes;
  }
  
  @Override
  public void stop() {
    // we do not want to close the cache, as there may be other
    // applications/webapps
    // using this cache. TODO do we want to close the regions that are created
    // by this application?
  }

  @Override
  public boolean isMinimalPutsEnabledByDefault() {
    // minimal puts is better for clustered cache
    return true;
  }

  @Override
  public AccessType getDefaultAccessType() {
    return AccessType.NONSTRICT_READ_WRITE;
  }

  @Override
  public long nextTimestamp() {
    log.debug("nextTimestamp called");
    // TODO use gemfire cache time here. (which tries to minimize clock skews)
    return Timestamper.next();
  }

  @Override
  public EntityRegion buildEntityRegion(String regionName,
      Properties properties, CacheDataDescription metadata)
      throws CacheException {
    // create the backing region
    log.debug("creating Entity region {} ", regionName);
    Region<Object, EntityWrapper> region = delegate.createRegion(regionName);
    GemFireEntityRegion r = new GemFireEntityRegion(region, isClient, metadata, this);
    this.entityRegionMap.put(regionName, r);
    return r;
  }

  @Override
  public CollectionRegion buildCollectionRegion(String regionName,
      Properties properties, CacheDataDescription metadata)
      throws CacheException {
    log.debug("creating collection region {}",regionName);
    Region<Object, EntityWrapper> region = delegate.createRegion(regionName);
    return new GemFireCollectionRegion(region, isClient, metadata, this);
  }

  @Override
  public QueryResultsRegion buildQueryResultsRegion(String regionName,
      Properties properties) throws CacheException {
    log.debug("Creating a query results region");
    Region region = getLocalRegionForQueryCache();
    return new GemFireQueryResultsRegion(region);
  }

  private Region getLocalRegionForQueryCache() {
    return getLocalRegion(GEMFIRE_QUERY_RESULTS_REGION_NAME);
  }
  
  private Region getLocalRegionForTimestampsCache() {
    return getLocalRegion(GEMFIRE_TIMESTAMPS_REGION_NAME);
  }
  
  private Region getLocalRegion(String regionName) {
    Region region = _cache.getRegion(regionName);
    if (region != null) {
      return region;
    }
    if (isClient) {
      ClientCache cc = (ClientCache)_cache;
      region = cc.createClientRegionFactory(ClientRegionShortcut.LOCAL_HEAP_LRU).create(regionName);
    } else {
      Cache c = (Cache)_cache;
      region = c.createRegionFactory(RegionShortcut.LOCAL_HEAP_LRU).create(regionName);
    }
    return region;
  }
  
  @Override
  public TimestampsRegion buildTimestampsRegion(String regionName,
      Properties properties) throws CacheException {
    Region region = getLocalRegionForTimestampsCache();
    return new GemFireQueryResultsRegion(region);
  }

  /**
   * Given an entity name, gets the region used to store
   * that entity.
   * @param name name of the entity
   * @return the entity region for the given entity name
   */
  public GemFireEntityRegion getEntityRegion(String name) {
    return this.entityRegionMap.get(name);
  }
}
