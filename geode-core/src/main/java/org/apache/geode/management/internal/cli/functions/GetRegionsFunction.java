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

import java.util.HashSet;
import java.util.Set;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.management.internal.cli.domain.RegionInformation;

/**
 * Function that retrieves regions hosted on every member
 */
public class GetRegionsFunction implements Function, InternalEntity {

  private static final long serialVersionUID = 1L;

  @Override
  public String getId() {
    // TODO Auto-generated method stub
    return GetRegionsFunction.class.toString();
  }

  @Override
  public void execute(FunctionContext functionContext) {
    try {
      Cache cache = CacheFactory.getAnyInstance();
      Set<Region<?, ?>> regions = cache.rootRegions(); // should never return a null

      if (regions == null || regions.isEmpty()) {
        functionContext.getResultSender().lastResult(null);
      } else {
        Set<RegionInformation> regionInformationSet = new HashSet<>();

        for (Region<?, ?> region : regions) {
          RegionInformation regInfo = new RegionInformation(region, true);
          regionInformationSet.add(regInfo);
        }
        functionContext.getResultSender().lastResult(regionInformationSet.toArray());
      }
    } catch (Exception e) {
      functionContext.getResultSender().sendException(e);
    }
  }

}
