/*
 * ========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.functions;

import java.io.Serializable;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.CacheLoader;
import com.gemstone.gemfire.cache.CacheWriter;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;

/**
 * 
 * @author Abhishek Chaudhari
 * @since 7.0
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
