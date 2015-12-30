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

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheWriter;
import com.gemstone.gemfire.cache.ExpirationAction;
import com.gemstone.gemfire.cache.ExpirationAttributes;
import com.gemstone.gemfire.cache.GemFireCache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.control.RebalanceFactory;
import com.gemstone.gemfire.cache.control.RebalanceOperation;
import com.gemstone.gemfire.cache.control.RebalanceResults;
import com.gemstone.gemfire.cache.control.ResourceManager;
import com.gemstone.gemfire.cache.partition.PartitionMemberInfo;
import com.gemstone.gemfire.cache.partition.PartitionRebalanceInfo;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXmlGenerator;
import com.gemstone.gemfire.internal.cache.xmlcache.RegionAttributesCreation;
import com.gemstone.gemfire.modules.gatewaydelta.GatewayDeltaForwarderCacheListener;
import com.gemstone.gemfire.modules.session.catalina.callback.SessionExpirationCacheListener;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CancellationException;

@SuppressWarnings({"deprecation", "unchecked"})
public class RegionHelper {

  public static final String NAME = "gemfire_modules";

  public static Region createRegion(Cache cache, RegionConfiguration configuration) {
    // Use the createRegion method so that the RegionAttributes creation can be reused by validate.
    RegionAttributes requestedRegionAttributes = getRegionAttributes(cache, configuration);
    Region region = cache.createRegion(configuration.getRegionName(), requestedRegionAttributes);

    // Log the cache xml if debugging is enabled. I'd like to be able to just
    // log the region, but that API is not available.
    if (configuration.getEnableDebugListener()) {
      cache.getLogger().info("Created new session region: " + region);
      cache.getLogger().info(generateCacheXml(cache));
    }
    return region;
  }

  public static void validateRegion(Cache cache, RegionConfiguration configuration, Region region) {
    // Get the attributes of the existing region
    RegionAttributes existingAttributes = region.getAttributes();

    // Create region attributes creation on existing region attributes.
    // The RAC is created to execute the sameAs method.
    RegionAttributesCreation existingRACreation = new RegionAttributesCreation(existingAttributes, false);

    // Create requested region attributes
    RegionAttributes requestedRegionAttributes = getRegionAttributes(cache, configuration);

    // Compare the two region attributes. This method either returns true or throws a RuntimeException.
    existingRACreation.sameAs(requestedRegionAttributes);
  }

  public static RebalanceResults rebalanceRegion(Region region) throws CancellationException, InterruptedException {
    String regionName = region.getName(); // FilterByName only looks at name and not full path
    if (!PartitionRegionHelper.isPartitionedRegion(region)) {
      StringBuilder builder = new StringBuilder();
      builder.append("Region ")
          .append(regionName)
          .append(" is not partitioned. Instead, it is ")
          .append(region.getAttributes().getDataPolicy())
          .append(". It can't be rebalanced.");
      throw new IllegalArgumentException(builder.toString());
    }

    // Rebalance the region
    ResourceManager resourceManager = region.getCache().getResourceManager();
    RebalanceFactory rebalanceFactory = resourceManager.createRebalanceFactory();
    Set<String> regionsToRebalance = new HashSet<String>();
    regionsToRebalance.add(regionName);
    rebalanceFactory.includeRegions(regionsToRebalance);
    RebalanceOperation rebalanceOperation = rebalanceFactory.start();

    // Return the results
    return rebalanceOperation.getResults();
  }

  public static RebalanceResults rebalanceCache(GemFireCache cache) throws CancellationException, InterruptedException {
    ResourceManager resourceManager = cache.getResourceManager();
    RebalanceFactory rebalanceFactory = resourceManager.createRebalanceFactory();
    RebalanceOperation rebalanceOperation = rebalanceFactory.start();
    return rebalanceOperation.getResults();
  }

  public static String generateCacheXml(Cache cache) {
    try {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw, true);
      CacheXmlGenerator.generate(cache, pw);
      pw.close();
      return sw.toString();
    } catch (Exception ex) {
      return "";
    }
  }

  private static RegionAttributes getRegionAttributes(Cache cache, RegionConfiguration configuration) {
    // Create the requested attributes
    RegionAttributes baseRequestedAttributes = cache.getRegionAttributes(configuration.getRegionAttributesId());
    if (baseRequestedAttributes == null) {
      throw new IllegalArgumentException(
          "No region attributes named " + configuration.getRegionAttributesId() + " are defined.");
    }
    AttributesFactory requestedFactory = new AttributesFactory(baseRequestedAttributes);

    // Set the expiration time and action if necessary
    int maxInactiveInterval = configuration.getMaxInactiveInterval();
    if (maxInactiveInterval != RegionConfiguration.DEFAULT_MAX_INACTIVE_INTERVAL) {
      requestedFactory.setStatisticsEnabled(true);
      if (configuration.getCustomExpiry() == null) {
        requestedFactory.setEntryIdleTimeout(new ExpirationAttributes(maxInactiveInterval, ExpirationAction.DESTROY));
      } else {
        requestedFactory.setCustomEntryIdleTimeout(configuration.getCustomExpiry());
      }
    }

    // Add the gateway delta region cache listener if necessary
    if (configuration.getEnableGatewayDeltaReplication()) {
      // Add the listener that forwards created/destroyed deltas to the gateway
      requestedFactory.addCacheListener(new GatewayDeltaForwarderCacheListener(cache));
    }

    // Enable gateway replication if necessary
    // TODO: Disabled for WAN
//    requestedFactory.setEnableGateway(configuration.getEnableGatewayReplication());

    // Add the debug cache listener if necessary
    if (configuration.getEnableDebugListener()) {
      requestedFactory.addCacheListener(new DebugCacheListener());
    }

    if (configuration.getSessionExpirationCacheListener()) {
      requestedFactory.addCacheListener(new SessionExpirationCacheListener());
    }

    // Add the cacheWriter if necessary
    if (configuration.getCacheWriterName() != null) {
      try {
        CacheWriter writer = (CacheWriter) Class.forName(configuration.getCacheWriterName()).newInstance();
        requestedFactory.setCacheWriter(writer);
      } catch (InstantiationException e) {
        throw new RuntimeException("Could not set a cacheWriter for the region", e);
      } catch (IllegalAccessException e) {
        throw new RuntimeException("Could not set a cacheWriter for the region", e);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException("Could not set a cacheWriter for the region", e);
      }
    }
    return requestedFactory.create();
  }

  private RegionHelper() {
  }

  public static String getRebalanceResultsMessage(RebalanceResults results) {
    StringBuilder builder = new StringBuilder();
    for (PartitionRebalanceInfo rebalanceInfo : results.getPartitionRebalanceDetails()) {
      // Log the overall results
      fillInRebalanceResultsSummary(builder, rebalanceInfo);

      // Log the 'Before' results
      fillInRebalanceResultsMemberDetails(builder, rebalanceInfo.getPartitionMemberDetailsBefore(), "Before");

      // Log the 'After' results
      fillInRebalanceResultsMemberDetails(builder, rebalanceInfo.getPartitionMemberDetailsAfter(), "After");
    }
    return builder.toString();
  }

  private static void fillInRebalanceResultsSummary(StringBuilder builder, PartitionRebalanceInfo rebalanceInfo) {
    builder.append("\nRebalanced region ")
        .append(rebalanceInfo.getRegionPath())
        .append(" in ")
        .append(rebalanceInfo.getTime())
        .append(" ms")

        .append("\nCreated ")
        .append(rebalanceInfo.getBucketCreatesCompleted())
        .append(" buckets containing ")
        .append(rebalanceInfo.getBucketCreateBytes())
        .append(" bytes in ")
        .append(rebalanceInfo.getBucketCreateTime())
        .append(" ms")

        .append("\nTransferred ")
        .append(rebalanceInfo.getBucketTransfersCompleted())
        .append(" buckets containing ")
        .append(rebalanceInfo.getBucketTransferBytes())
        .append(" bytes in ")
        .append(rebalanceInfo.getBucketTransferTime())
        .append(" ms")

        .append("\nTransferred ")
        .append(rebalanceInfo.getPrimaryTransfersCompleted())
        .append(" primary buckets in ")
        .append(rebalanceInfo.getPrimaryTransferTime())
        .append(" ms");
  }

  private static void fillInRebalanceResultsMemberDetails(StringBuilder builder, Set<PartitionMemberInfo> memberInfoSet,
      String when) {
    builder.append("\nMember Info ").append(when).append(" Rebalance:\n");
    for (PartitionMemberInfo info : memberInfoSet) {
      builder.append("\tdistributedMember=")
          .append(info.getDistributedMember())
          .append(", configuredMaxMemory=")
          .append(info.getConfiguredMaxMemory())
          .append(", size=")
          .append(info.getSize())
          .append(", bucketCount=")
          .append(info.getBucketCount())
          .append(", primaryCount=")
          .append(info.getPrimaryCount());
    }
  }
}
