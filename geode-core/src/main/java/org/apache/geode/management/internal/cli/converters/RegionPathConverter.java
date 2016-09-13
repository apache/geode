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
package org.apache.geode.management.internal.cli.converters;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.internal.cli.shell.Gfsh;

import org.springframework.shell.core.Completion;
import org.springframework.shell.core.Converter;
import org.springframework.shell.core.MethodTarget;

/**
 * 
 * @since GemFire 7.0
 */
public class RegionPathConverter implements Converter<String> {

  public static final String DEFAULT_APP_CONTEXT_PATH = "";

  @Override
  public boolean supports(Class<?> type, String optionContext) {
    return String.class.equals(type) && ConverterHint.REGIONPATH.equals(optionContext);
  }

  @Override
  public String convertFromText(String value, Class<?> targetType,
      String optionContext) {
    return value;
  }

  @Override
  public boolean getAllPossibleValues(List<Completion> completions,
      Class<?> targetType, String existingData, String optionContext,
      MethodTarget target) {
    if (String.class.equals(targetType) && ConverterHint.REGIONPATH.equals(optionContext)) {
      Set<String> regionPathSet = getAllRegionPaths();
      Gfsh gfsh = Gfsh.getCurrentInstance();
      String currentContextPath = "";
      if (gfsh != null) {
        currentContextPath = gfsh.getEnvProperty(Gfsh.ENV_APP_CONTEXT_PATH);
        if (currentContextPath != null && !org.apache.geode.management.internal.cli.converters.RegionPathConverter.DEFAULT_APP_CONTEXT_PATH.equals(currentContextPath)) {
          regionPathSet.remove(currentContextPath);
          regionPathSet.add(org.apache.geode.management.internal.cli.converters.RegionPathConverter.DEFAULT_APP_CONTEXT_PATH);
        }
      }

      for (String regionPath : regionPathSet) {
        if (existingData != null) {
          if (regionPath.startsWith(existingData)) {
            completions.add(new Completion(regionPath));
          }
        } else {
          completions.add(new Completion(regionPath));
        }
      }
    }

    return !completions.isEmpty();
  }

  Set<String> getAllRegionPaths() {
    Set<String> regionPathSet = Collections.emptySet();

    Gfsh gfsh = Gfsh.getCurrentInstance();
    if (gfsh != null && gfsh.isConnectedAndReady()) {
      String[] regionPaths = gfsh.getOperationInvoker().getDistributedSystemMXBean().listAllRegionPaths();
      if (regionPaths != null && regionPaths.length > 0) {
        regionPathSet = new TreeSet<String>();
        for (String regionPath : regionPaths) {
          if (regionPath != null) { // Not needed after 46387/46502 are addressed
            regionPathSet.add(regionPath);
          }
        }
      }
    }

    return regionPathSet;
  }

}
