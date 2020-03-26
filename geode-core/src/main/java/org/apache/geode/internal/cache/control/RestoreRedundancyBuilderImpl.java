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
package org.apache.geode.internal.cache.control;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.control.RestoreRedundancyBuilder;
import org.apache.geode.cache.control.RestoreRedundancyResults;
import org.apache.geode.cache.partition.PartitionRebalanceInfo;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.partitioned.PartitionedRegionRebalanceOp;
import org.apache.geode.internal.cache.partitioned.rebalance.RestoreRedundancyDirector;
import org.apache.geode.internal.serialization.Version;
import org.apache.geode.logging.internal.log4j.api.LogService;

class RestoreRedundancyBuilderImpl implements RestoreRedundancyBuilder {

  private final InternalCache cache;
  private final InternalResourceManager manager;
  private Set<String> includedRegions;
  private Set<String> excludedRegions;
  private boolean shouldNotReassign = false;
  private ScheduledExecutorService executor;

  public RestoreRedundancyBuilderImpl(InternalCache cache) {
    this.cache = cache;
    this.manager = cache.getInternalResourceManager();
    this.executor = this.manager.getExecutor();
  }

  @Override
  public RestoreRedundancyBuilder includeRegions(Set<String> regions) {
    this.includedRegions = regions;
    return this;
  }

  @Override
  public RestoreRedundancyBuilder excludeRegions(Set<String> regions) {
    this.excludedRegions = regions;
    return this;
  }

  @Override
  public RestoreRedundancyBuilder doNotReassignPrimaries(boolean shouldNotReassign) {
    this.shouldNotReassign = shouldNotReassign;
    return this;
  }

  @Override
  public CompletableFuture<RestoreRedundancyResults> start() {
    if (hasMemberOlderThanGeode_1_13_0()) {
      return CompletableFuture.completedFuture(getErrorRestoreRedundancyResult());
    } else {
      RegionFilter filter = getRegionFilter();
      long start = manager.getStats().startRestoreRedundancy();

      // Create a list of completable futures for each restore redundancy operation
      List<CompletableFuture<RestoreRedundancyResults>> regionFutures =
          cache.getPartitionedRegions().stream()
              .filter(filter::include)
              .map(this::getRedundancyOpFuture)
              .collect(Collectors.toList());

      // Create a single completable future which completes when all of the restore redundancy
      // futures return
      CompletableFuture<Void> combinedFuture =
          CompletableFuture.allOf(regionFutures.toArray(new CompletableFuture[0]));

      // Once all restore redundancy futures have returned, combine the results from each into one
      // results object
      CompletableFuture<RestoreRedundancyResults> resultsFuture =
          getResultsFuture(regionFutures, combinedFuture);

      // Once results have been collected and combined, mark the operation as finished
      resultsFuture.thenRun(() -> {
        manager.removeInProgressRestoreRedundancy(resultsFuture);
        manager.getStats().endRestoreRedundancy(start);
      });

      manager.addInProgressRestoreRedundancy(resultsFuture);
      return resultsFuture;
    }
  }

  @Override
  public RestoreRedundancyResults redundancyStatus() {
    if (hasMemberOlderThanGeode_1_13_0()) {
      return getErrorRestoreRedundancyResult();
    } else {
      RegionFilter filter = getRegionFilter();
      RestoreRedundancyResults results = getEmptyRestoreRedundancyResults();
      cache.getPartitionedRegions().stream().filter(filter::include)
          .forEach(region -> results.addRegionResult(getRegionResult(region)));
      return results;
    }
  }

  private boolean hasMemberOlderThanGeode_1_13_0() {
    return cache.getMembers().stream()
        .map(InternalDistributedMember.class::cast)
        .map(InternalDistributedMember::getVersionObject)
        .anyMatch(memberVersion -> memberVersion.compareTo(Version.GEODE_1_13_0) < 0);
  }

  RestoreRedundancyResults doRestoreRedundancy(PartitionedRegion region) {
    try {
      PartitionedRegionRebalanceOp op = getPartitionedRegionRebalanceOp(region);

      cache.getCancelCriterion().checkCancelInProgress(null);

      Set<PartitionRebalanceInfo> detailSet;
      try {
        detailSet = op.execute();

        RestoreRedundancyResults results = getEmptyRestoreRedundancyResults();
        // No work was done, either because redundancy was not impaired or because colocation
        // was not complete
        if (detailSet.isEmpty()) {
          results.addRegionResult(getRegionResult(region));
        } else {
          for (PartitionRebalanceInfo details : detailSet) {
            PartitionedRegion detailRegion =
                (PartitionedRegion) cache.getRegion(details.getRegionPath());
            results.addRegionResult(getRegionResult(detailRegion));
            results.addPrimaryReassignmentDetails(details);
          }
        }
        return results;
      } catch (RuntimeException ex) {
        LogService.getLogger().debug("Unexpected exception in restoring redundancy: {}",
            ex.getMessage(), ex);
        throw ex;
      }
    } catch (RegionDestroyedException ex) {
      // We can ignore this and go on to the next region, so return an empty results object
      return getEmptyRestoreRedundancyResults();
    }
  }

  RestoreRedundancyResults getRestoreRedundancyResults(
      List<CompletableFuture<RestoreRedundancyResults>> regionFutures) {
    RestoreRedundancyResults finalResult = getEmptyRestoreRedundancyResults();
    regionFutures.stream()
        .map(CompletableFuture::join)
        .forEach(finalResult::addRegionResults);
    return finalResult;
  }

  // Extracted for testing
  RegionFilter getRegionFilter() {
    return new FilterByPath(this.includedRegions, this.excludedRegions);
  }

  // Extracted for testing
  CompletableFuture<RestoreRedundancyResults> getRedundancyOpFuture(PartitionedRegion region) {
    return CompletableFuture.supplyAsync(() -> doRestoreRedundancy(region), executor);
  }

  // Extracted for testing
  PartitionedRegionRebalanceOp getPartitionedRegionRebalanceOp(PartitionedRegion region) {
    return new PartitionedRegionRebalanceOp(region, false,
        new RestoreRedundancyDirector(shouldNotReassign), true, false, new AtomicBoolean(),
        manager.getStats());
  }

  // Extracted for testing
  RestoreRedundancyResults getEmptyRestoreRedundancyResults() {
    return new RestoreRedundancyResultsImpl();
  }

  // Extracted for testing
  RestoreRedundancyRegionResult getRegionResult(PartitionedRegion region) {
    return new RestoreRedundancyRegionResult(region);
  }

  // Extracted for testing
  CompletableFuture<RestoreRedundancyResults> getResultsFuture(
      List<CompletableFuture<RestoreRedundancyResults>> regionFutures,
      CompletableFuture<Void> combinedFuture) {
    return combinedFuture.thenApplyAsync(voidd -> getRestoreRedundancyResults(regionFutures),
        executor);
  }

  private RestoreRedundancyResults getErrorRestoreRedundancyResult() {
    return new RestoreRedundancyResults() {
      @Override
      public void addRegionResults(RestoreRedundancyResults results) {}

      @Override
      public void addPrimaryReassignmentDetails(PartitionRebalanceInfo details) {}

      @Override
      public void addRegionResult(RestoreRedundancyRegionResult regionResult) {}

      @Override
      public Status getStatus() {
        return Status.ERROR;
      }

      @Override
      public String getMessage() {
        return "Restore redundancy operations are not supported on versions older than "
            + Version.GEODE_1_13_0.toString();
      }

      @Override
      public RestoreRedundancyRegionResult getRegionResult(String regionName) {
        return null;
      }

      @Override
      public Map<String, RestoreRedundancyRegionResult> getZeroRedundancyRegionResults() {
        return new HashMap<>();
      }

      @Override
      public Map<String, RestoreRedundancyRegionResult> getUnderRedundancyRegionResults() {
        return new HashMap<>();
      }

      @Override
      public Map<String, RestoreRedundancyRegionResult> getSatisfiedRedundancyRegionResults() {
        return new HashMap<>();
      }

      @Override
      public Map<String, RestoreRedundancyRegionResult> getRegionResults() {
        return new HashMap<>();
      }

      @Override
      public int getTotalPrimaryTransfersCompleted() {
        return 0;
      }

      @Override
      public long getTotalPrimaryTransferTime() {
        return 0;
      }
    };
  }
}
