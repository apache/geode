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
package org.apache.geode.management.internal.cli.functions;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.cache.AbstractRegion;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.domain.ClassName;
import org.apache.geode.management.internal.cli.i18n.CliStrings;

/**
 *
 * @since GemFire 7.0
 */
public class FetchRegionAttributesFunction implements InternalFunction {
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
      Cache cache = context.getCache();
      String regionPath = (String) context.getArguments();
      if (regionPath == null) {
        throw new IllegalArgumentException(
            CliStrings.CREATE_REGION__MSG__SPECIFY_VALID_REGION_PATH);
      }
      RegionAttributesWrapper regionAttributesWrapper = getRegionAttributes(cache, regionPath);
      context.getResultSender().lastResult(regionAttributesWrapper);
    } catch (IllegalArgumentException e) {
      if (logger.isDebugEnabled()) {
        logger.debug(e.getMessage(), e);
      }
      context.getResultSender().lastResult(e);
    }
  }

  @SuppressWarnings("deprecation")
  public static RegionAttributesWrapper getRegionAttributes(Cache cache, String regionPath) {
    AbstractRegion foundRegion = (AbstractRegion) cache.getRegion(regionPath);

    if (foundRegion == null) {
      throw new IllegalArgumentException(CliStrings.format(
          CliStrings.CREATE_REGION__MSG__SPECIFY_VALID_REGION_PATH_FOR_0_REGIONPATH_1_NOT_FOUND,
          CliStrings.CREATE_REGION__USEATTRIBUTESFROM, regionPath));
    }

    AttributesFactory afactory = new AttributesFactory(foundRegion.getAttributes());
    RegionAttributesWrapper result = new RegionAttributesWrapper();

    CacheListener[] cacheListeners = foundRegion.getCacheListeners();
    List existingCacheListeners = Arrays.stream(cacheListeners)
        .map((c) -> new ClassName(c.getClass().getName())).collect(Collectors.toList());

    result.setCacheListenerClasses(existingCacheListeners);
    afactory.initCacheListeners(null);

    if (foundRegion.getCacheLoader() != null) {
      result
          .setCacheLoaderClass(new ClassName<>(foundRegion.getCacheLoader().getClass().getName()));
      afactory.setCacheLoader(null);
    }

    if (foundRegion.getCacheWriter() != null) {
      result
          .setCacheWriterClass(new ClassName<>(foundRegion.getCacheWriter().getClass().getName()));
      afactory.setCacheWriter(null);
    }

    if (foundRegion.getCompressor() != null) {
      result.setCompressorClass(foundRegion.getCompressor().getClass().getName());
      afactory.setCompressor(null);
    }

    if (foundRegion.getKeyConstraint() != null) {
      result.setKeyConstraintClass(foundRegion.getKeyConstraint().getName());
      afactory.setKeyConstraint(null);
    }

    if (foundRegion.getValueConstraint() != null) {
      result.setValueConstraintClass(foundRegion.getValueConstraint().getName());
      afactory.setValueConstraint(null);
    }

    result.setRegionAttributes(afactory.create());
    return result;
  }

  @Override
  public String getId() {
    return ID;
  }
}
