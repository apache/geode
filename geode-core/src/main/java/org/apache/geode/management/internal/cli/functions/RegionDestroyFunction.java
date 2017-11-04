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

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.internal.cache.xmlcache.CacheXml;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;

/**
 *
 * @since GemFire 7.0
 */
public class RegionDestroyFunction implements Function, InternalEntity {
  private static final long serialVersionUID = 9172773671865750685L;

  public static final RegionDestroyFunction INSTANCE = new RegionDestroyFunction();

  private static final String ID = RegionDestroyFunction.class.getName();

  @Override
  public boolean hasResult() {
    return true;
  }

  @Override
  public void execute(FunctionContext context) {
    String regionPath = null;
    try {
      String functionId = context.getFunctionId();
      Object arguments = context.getArguments();

      if (!getId().equals(functionId) || arguments == null) {
        context.getResultSender().lastResult(new CliFunctionResult("", false,
            "Function Id mismatch or arguments is not available."));
        return;
      }

      regionPath = (String) arguments;
      Cache cache = context.getCache();
      Region<?, ?> region = cache.getRegion(regionPath);
      // if the region is a distributed region, and is already destroyed by another member
      if (region == null) {
        context.getResultSender().lastResult(new CliFunctionResult("", true, "SUCCESS"));
        return;
      }

      region.destroyRegion();
      String regionName =
          regionPath.startsWith(Region.SEPARATOR) ? regionPath.substring(1) : regionPath;
      XmlEntity xmlEntity = new XmlEntity(CacheXml.REGION, "name", regionName);
      context.getResultSender().lastResult(new CliFunctionResult("", xmlEntity, regionPath));

    } catch (IllegalStateException ex) {
      // user is trying to destroy something that can't destroyed.
      context.getResultSender().lastResult(new CliFunctionResult("", false, ex.getMessage()));
    } catch (Exception ex) {
      context.getResultSender()
          .lastResult(new CliFunctionResult("", ex, "failed to destroy " + regionPath));
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
