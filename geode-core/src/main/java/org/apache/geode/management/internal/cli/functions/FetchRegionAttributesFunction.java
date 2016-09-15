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
package org.apache.geode.management.internal.cli.functions;

import java.io.Serializable;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.internal.cli.i18n.CliStrings;

/**
 * 
 * @since GemFire 7.0
 */
public class FetchRegionAttributesFunction extends FunctionAdapter {
  private static final Logger logger = LogService.getLogger();
  
  private static final long serialVersionUID = 4366812590788342070L;

  private static final String ID = FetchRegionAttributesFunction.class.getName();

  public static FetchRegionAttributesFunction INSTANCE = new FetchRegionAttributesFunction();

  @Override
  public boolean isHA() {
    return false;
  }

  @Override
  public void execute(FunctionContext context) {
    try {
      String regionPath = (String) context.getArguments();
      if (regionPath == null) {
        throw new IllegalArgumentException(CliStrings.CREATE_REGION__MSG__SPECIFY_VALID_REGION_PATH);
      }
      FetchRegionAttributesFunctionResult<?, ?> result = getRegionAttributes(regionPath);
      context.getResultSender().lastResult(result);
    } catch (IllegalArgumentException e) {
      if (logger.isDebugEnabled()) {
        logger.debug(e.getMessage(), e);
      }
      context.getResultSender().lastResult(e);
    }
  }

  @SuppressWarnings("deprecation")
  public static <K, V> FetchRegionAttributesFunctionResult<K, V> getRegionAttributes(String regionPath) {
    Cache cache = CacheFactory.getAnyInstance();
    Region<K, V> foundRegion = cache.getRegion(regionPath);

    if (foundRegion == null) {
      throw new IllegalArgumentException(CliStrings.format(CliStrings.CREATE_REGION__MSG__SPECIFY_VALID_REGION_PATH_FOR_0_REGIONPATH_1_NOT_FOUND, new Object[] {CliStrings.CREATE_REGION__USEATTRIBUTESFROM, regionPath}));
    }

    // Using AttributesFactory to get the serializable RegionAttributes 
    // Is there a better way?
    AttributesFactory<K, V> afactory = new AttributesFactory<K, V>(foundRegion.getAttributes());
    FetchRegionAttributesFunctionResult<K, V> result = new FetchRegionAttributesFunctionResult<K, V>(afactory);
    return result;
  }

  @Override
  public String getId() {
    return ID;
  }
  
  public static class FetchRegionAttributesFunctionResult<K, V> implements Serializable {
    private static final long serialVersionUID = -3970828263897978845L;

    private RegionAttributes<K, V> regionAttributes;
    private String[] cacheListenerClasses;
    private String cacheLoaderClass;
    private String cacheWriterClass;

    @SuppressWarnings("deprecation")
    public FetchRegionAttributesFunctionResult(AttributesFactory<K, V> afactory) {
      this.regionAttributes = afactory.create();

      CacheListener<K, V>[] cacheListeners = this.regionAttributes.getCacheListeners();
      if (cacheListeners != null && cacheListeners.length != 0) {
        cacheListenerClasses = new String[cacheListeners.length];
        for (int i = 0; i < cacheListeners.length; i++) {
          cacheListenerClasses[i] = cacheListeners[i].getClass().getName();
        }
        afactory.initCacheListeners(null);
      }
      CacheLoader<K, V> cacheLoader = this.regionAttributes.getCacheLoader();
      if (cacheLoader != null) {
        cacheLoaderClass = cacheLoader.getClass().getName();
        afactory.setCacheLoader(null);
      }
      CacheWriter<K, V> cacheWriter = this.regionAttributes.getCacheWriter();
      if (cacheWriter != null) {
        cacheWriterClass = cacheWriter.getClass().getName();
        afactory.setCacheWriter(null);
      }

      // recreate attributes
      this.regionAttributes = afactory.create();
    }

    public RegionAttributes<K, V> getRegionAttributes() {
      return regionAttributes;
    }

    public String[] getCacheListenerClasses() {
      return cacheListenerClasses;
    }

    public String getCacheLoaderClass() {
      return cacheLoaderClass;
    }

    public String getCacheWriterClass() {
      return cacheWriterClass;
    }
  }
}
