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

import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.internal.cache.xmlcache.CacheXml;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.management.internal.functions.CliFunctionResult;

/**
 *
 * @since GemFire 7.0
 */
public class RegionDestroyFunction implements InternalFunction<String> {
  private static final long serialVersionUID = 9172773671865750685L;

  @Immutable
  public static final RegionDestroyFunction INSTANCE = new RegionDestroyFunction();

  private static final String ID = RegionDestroyFunction.class.getName();

  @Override
  public boolean hasResult() {
    return true;
  }

  @Override
  @SuppressWarnings("deprecation")
  public void execute(FunctionContext<String> context) {
    String regionPath = context.getArguments();
    String memberName = context.getMemberName();

    try {
      String functionId = context.getFunctionId();

      if (!getId().equals(functionId) || regionPath == null) {
        context.getResultSender().lastResult(new CliFunctionResult("", false,
            "Function Id mismatch or arguments is not available."));
        return;
      }

      Cache cache = ((InternalCache) context.getCache()).getCacheForProcessingClientRequests();
      Region<?, ?> region = cache.getRegion(regionPath);
      // the region is already destroyed by another member
      if (region == null) {
        context.getResultSender().lastResult(new CliFunctionResult(memberName, true,
            String.format("Region '%s' already destroyed", regionPath)));
        return;
      }

      region.destroyRegion();

      String regionName =
          regionPath.startsWith(Region.SEPARATOR) ? regionPath.substring(1) : regionPath;
      XmlEntity xmlEntity = new XmlEntity(CacheXml.REGION, "name", regionName);
      context.getResultSender().lastResult(new CliFunctionResult(memberName, xmlEntity,
          String.format("Region '%s' destroyed successfully", regionPath)));

    } catch (IllegalStateException ex) {
      // user is trying to destroy something that can't destroyed, like co-location
      context.getResultSender()
          .lastResult(new CliFunctionResult(memberName, false, ex.getMessage()));
    } catch (RegionDestroyedException ex) {
      context.getResultSender().lastResult(new CliFunctionResult(memberName, true,
          String.format("Region '%s' already destroyed", regionPath)));
    } catch (Exception ex) {
      LogService.getLogger().error(ex.getMessage(), ex);
      context.getResultSender().lastResult(new CliFunctionResult(memberName, ex, ex.getMessage()));
    }
  }

  @Override
  public String getId() {
    return ID;
  }

  @Override
  public boolean optimizeForWrite() {
    return false;
  }

  @Override
  public boolean isHA() {
    return false;
  }
}
