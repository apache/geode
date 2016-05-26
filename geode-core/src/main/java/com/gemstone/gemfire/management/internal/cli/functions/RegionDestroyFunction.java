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
package com.gemstone.gemfire.management.internal.cli.functions;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXml;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.configuration.domain.XmlEntity;

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
      if (getId().equals(functionId)) {
        Object arguments = context.getArguments();
        if (arguments != null) {
          regionPath = (String) arguments;
          Cache cache = CacheFactory.getAnyInstance();
          Region<?, ?> region = cache.getRegion(regionPath);
          region.destroyRegion();
          String regionName = regionPath.startsWith(Region.SEPARATOR) ? regionPath.substring(1) : regionPath;
          XmlEntity xmlEntity = new XmlEntity(CacheXml.REGION, "name", regionName);
          context.getResultSender().lastResult(new CliFunctionResult("", xmlEntity, regionPath));
        }
      }
      context.getResultSender().lastResult(new CliFunctionResult("", false, "FAILURE"));
    } catch (IllegalStateException e) {
      context.getResultSender().lastResult(new CliFunctionResult("", e, null));
    } catch (Exception ex) {
      context.getResultSender().lastResult(new CliFunctionResult("", new RuntimeException(CliStrings.format(CliStrings.DESTROY_REGION__MSG__ERROR_WHILE_DESTROYING_REGION_0_REASON_1, new Object[] {regionPath, ex.getMessage()})), null));
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
