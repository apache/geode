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
package org.apache.geode.management.internal.cli.converters;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.shell.core.Completion;
import org.springframework.shell.core.Converter;
import org.springframework.shell.core.MethodTarget;

import org.apache.geode.cache.Region;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.internal.cli.shell.Gfsh;

/**
 *
 * @since GemFire 7.0
 */
public class RegionPathConverter implements Converter<String> {
  @Override
  public boolean supports(Class<?> type, String optionContext) {
    return String.class.equals(type) && optionContext.contains(ConverterHint.REGION_PATH);
  }

  @Override
  public String convertFromText(String value, Class<?> targetType, String optionContext) {
    // When value is null, this should not be called. this is here for safety reasons
    if (value == null) {
      return null;
    }

    if (value.equals(Region.SEPARATOR)) {
      throw new IllegalArgumentException("invalid region path: " + value);
    }

    if (!value.startsWith(Region.SEPARATOR)) {
      value = Region.SEPARATOR + value;
    }
    return value;
  }

  @Override
  public boolean getAllPossibleValues(List<Completion> completions, Class<?> targetType,
      String existingData, String optionContext, MethodTarget target) {
    Set<String> regionPathSet = getAllRegionPaths();

    for (String regionPath : regionPathSet) {
      if (existingData != null) {
        if (regionPath.startsWith(existingData)) {
          completions.add(new Completion(regionPath));
        }
      } else {
        completions.add(new Completion(regionPath));
      }
    }

    return !completions.isEmpty();
  }

  public Set<String> getAllRegionPaths() {
    Set<String> regionPathSet = Collections.emptySet();
    Gfsh gfsh = Gfsh.getCurrentInstance();
    if (gfsh != null && gfsh.isConnectedAndReady()) {
      String[] regionPaths =
          gfsh.getOperationInvoker().getDistributedSystemMXBean().listAllRegionPaths();
      regionPathSet = Arrays.stream(regionPaths).collect(Collectors.toSet());
    }
    return regionPathSet;
  }

}
