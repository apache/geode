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

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.internal.cli.i18n.CliStrings;

/**
 *
 * @since GemFire 7.0
 */
public class FetchRegionAttributesFunction implements Function, InternalEntity {
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
      RegionAttributes<?, ?> result = getRegionAttributes(cache, regionPath);
      context.getResultSender().lastResult(result);
    } catch (IllegalArgumentException e) {
      if (logger.isDebugEnabled()) {
        logger.debug(e.getMessage(), e);
      }
      context.getResultSender().lastResult(e);
    }
  }

  @SuppressWarnings("deprecation")
  public static <K, V> RegionAttributes<K, V> getRegionAttributes(Cache cache, String regionPath) {
    Region<K, V> foundRegion = cache.getRegion(regionPath);

    if (foundRegion == null) {
      throw new IllegalArgumentException(CliStrings.format(
          CliStrings.CREATE_REGION__MSG__SPECIFY_VALID_REGION_PATH_FOR_0_REGIONPATH_1_NOT_FOUND,
          new Object[] {CliStrings.CREATE_REGION__USEATTRIBUTESFROM, regionPath}));
    }

    // Using AttributesFactory to get the serializable RegionAttributes
    // Is there a better way?
    AttributesFactory<K, V> afactory = new AttributesFactory<K, V>(foundRegion.getAttributes());
    return afactory.create();
  }

  @Override
  public String getId() {
    return ID;
  }
}
