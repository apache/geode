/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.converters;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import com.gemstone.gemfire.management.cli.ConverterHint;
import com.gemstone.gemfire.management.internal.cli.shell.Gfsh;

import org.springframework.shell.core.Completion;
import org.springframework.shell.core.Converter;
import org.springframework.shell.core.MethodTarget;

/**
 * 
 * @author Abhishek Chaudhari
 * @since 7.0
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
        if (currentContextPath != null && !com.gemstone.gemfire.management.internal.cli.converters.RegionPathConverter.DEFAULT_APP_CONTEXT_PATH.equals(currentContextPath)) {
          regionPathSet.remove(currentContextPath);
          regionPathSet.add(com.gemstone.gemfire.management.internal.cli.converters.RegionPathConverter.DEFAULT_APP_CONTEXT_PATH);
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
