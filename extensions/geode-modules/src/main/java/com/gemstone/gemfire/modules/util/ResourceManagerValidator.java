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
package com.gemstone.gemfire.modules.util;

import com.gemstone.gemfire.cache.GemFireCache;
import com.gemstone.gemfire.cache.control.ResourceManager;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ResourceManagerValidator {

  private static final Pattern DIGIT_PATTERN = Pattern.compile("(\\d+|[^\\d]+)");

  public static void validateJavaStartupParameters(GemFireCache cache) {
    // Get the input arguments
    ResourceManager rm = cache.getResourceManager();
    RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();
    List<String> inputArguments = runtimeBean.getInputArguments();
    if (cache.getLogger().fineEnabled()) {
      cache.getLogger().fine("Full input java arguments: " + inputArguments);
    }

    // Validate the arguments based on VM vendor
    String vmVendor = runtimeBean.getVmVendor();
    if (vmVendor.startsWith("Sun") || vmVendor.startsWith("Apple")) {
      // java.vm.vendor = Sun Microsystems Inc. || java.vm.vendor = Apple Inc.
      validateSunArguments(cache, rm, inputArguments);
    } else if (vmVendor.startsWith("IBM")) {
      // java.vm.vendor = IBM Corporation
      // TODO validate IBM input arguments
    } else if (vmVendor.startsWith("BEA")) {
      // java.vm.vendor = BEA Systems, Inc.
      // TODO validate JRockit input arguments
    }
  }

  private static void validateSunArguments(GemFireCache cache, ResourceManager rm, List<String> inputArguments) {
    // Retrieve the -Xms, -Xmx, UseConcMarkSweepGC and CMSInitiatingOccupancyFraction arguments
    String dashXms = null, dashXmx = null, useCMS = null, cmsIOF = null;
    for (String argument : inputArguments) {
      if (argument.startsWith("-Xms")) {
        dashXms = argument;
      } else if (argument.startsWith("-Xmx")) {
        dashXmx = argument;
      } else if (argument.equals("-XX:+UseConcMarkSweepGC")) {
        useCMS = argument;
      } else if (argument.startsWith("-XX:CMSInitiatingOccupancyFraction")) {
        cmsIOF = argument;
      }
    }
    if (cache.getLogger().fineEnabled()) {
      StringBuilder builder = new StringBuilder();
      builder.append("Relevant input java arguments: ")
          .append("dashXms=")
          .append(dashXms)
          .append("; dashXmx=")
          .append(dashXmx)
          .append("; useCMS=")
          .append(useCMS)
          .append("; cmsIOF=")
          .append(cmsIOF);
      cache.getLogger().fine(builder.toString());
    }

    // Validate the heap parameters
    validateJavaHeapParameters(cache, dashXms, dashXmx);

    // Verify CMS is specified
    verifyCMSGC(cache, useCMS);

    // Verify CMSInitiatingOccupancyFraction is specified
    verifyCMSInitiatingOccupancyFraction(cache, rm, cmsIOF);
  }

  private static void validateJavaHeapParameters(GemFireCache cache, String dashXms, String dashXmx) {
    if (dashXms == null) {
      cache.getLogger()
          .warning(
              "Setting the initial size of the heap (configured using -Xms) is recommended so that GemFire cache eviction is optimal");
    } else if (dashXmx == null) {
      cache.getLogger()
          .warning(
              "Setting the maximum size of the heap (configured using -Xmx) is recommended so that GemFire cache eviction is optimal");
    } else {
      // Neither heap parameter is null. Parse them and verify they are the same.
      List<String> dashXmsList = splitAtDigits(dashXms);
      String dashXmsStr = dashXmsList.get(1);
      List<String> dashXmxList = splitAtDigits(dashXmx);
      String dashXmxStr = dashXmxList.get(1);
      if (!dashXmsStr.equals(dashXmxStr)) {
        StringBuilder builder = new StringBuilder();
        builder.append("Setting the initial (")
            .append(dashXmsStr)
            .append(dashXmsList.get(2))
            .append(") and maximum (")
            .append(dashXmxStr)
            .append(dashXmxList.get(2))
            .append(") sizes of the heap the same is recommended so that GemFire cache eviction is optimal");
        cache.getLogger().warning(builder.toString());
      }
    }
  }

  private static void verifyCMSGC(GemFireCache cache, String useCMS) {
    if (useCMS == null) {
      cache.getLogger()
          .warning(
              "Using the concurrent garbage collector (configured using -XX:+UseConcMarkSweepGC) is recommended so that GemFire cache eviction is optimal");
    }
  }

  private static void verifyCMSInitiatingOccupancyFraction(GemFireCache cache, ResourceManager rm, String cmsIOF) {
    if (cmsIOF == null) {
      cache.getLogger()
          .warning(
              "Setting the CMS initiating occupancy fraction (configured using -XX:CMSInitiatingOccupancyFraction=N) is recommended so that GemFire cache eviction is optimal");
    } else {
      // Parse the CMSInitiatingOccupancyFraction. Verify it is less than both eviction and critical thresholds.
      int cmsIOFVal = Integer.parseInt(cmsIOF.split("=")[1]);
      float currentEvictionHeapPercentage = rm.getEvictionHeapPercentage();
      if (currentEvictionHeapPercentage != 0 && currentEvictionHeapPercentage < cmsIOFVal) {
        cache.getLogger()
            .warning(
                "Setting the CMS initiating occupancy fraction (" + cmsIOFVal + ") less than the eviction heap percentage (" + currentEvictionHeapPercentage + ") is recommended so that GemFire cache eviction is optimal");
      }
      float currentCriticalHeapPercentage = rm.getCriticalHeapPercentage();
      if (currentCriticalHeapPercentage != 0 && currentCriticalHeapPercentage < cmsIOFVal) {
        cache.getLogger()
            .warning(
                "Setting the CMS initiating occupancy fraction (" + cmsIOFVal + ") less than the critical heap percentage (" + currentCriticalHeapPercentage + ") is recommended so that GemFire cache eviction is optimal");
      }
    }
  }

  private static List<String> splitAtDigits(String input) {
    Matcher matcher = DIGIT_PATTERN.matcher(input);
    List<String> result = new ArrayList<String>();
    while (matcher.find()) {
      result.add(matcher.group());
    }
    return result;
  }

  private ResourceManagerValidator() {
  }

}
