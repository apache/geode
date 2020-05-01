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

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.control.RegionRedundancyStatus;
import org.apache.geode.cache.control.RestoreRedundancyOperation;
import org.apache.geode.cache.control.RestoreRedundancyResults;
import org.apache.geode.cache.partition.PartitionRebalanceInfo;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.partitioned.PartitionedRegionRebalanceOp;
import org.apache.geode.internal.cache.partitioned.rebalance.CompositeDirector;
import org.apache.geode.logging.internal.log4j.api.LogService;

class RestoreRedundancyOperationImpl implements RestoreRedundancyOperation {

  private final InternalCache cache;
  private final InternalResourceManager manager;
  private Set<String> includedRegions;
  private Set<String> excludedRegions;
  private boolean shouldReassign = true;
  private ScheduledExecutorService executor;

  public RestoreRedundancyOperationImpl(InternalCache cache) {
    this.cache = cache;
    this.manager = cache.getInternalResourceManager();
    this.executor = this.manager.getExecutor();
  }

  @Override
  public RestoreRedundancyOperation includeRegions(Set<String> regions) {
    this.includedRegions = regions;
    return this;
  }

  @Override
  public RestoreRedundancyOperation excludeRegions(Set<String> regions) {
    this.excludedRegions = regions;
    return this;
  }

  @Override
  public RestoreRedundancyOperation shouldReassignPrimaries(boolean shouldReassign) {
    this.shouldReassign = shouldReassign;
    return this;
  }

  @Override
  public CompletableFuture<RestoreRedundancyResults> start() {
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

  @Override
  public RestoreRedundancyResults redundancyStatus() {
    RegionFilter filter = getRegionFilter();
    RestoreRedundancyResultsImpl results = getEmptyRestoreRedundancyResults();
    cache.getPartitionedRegions().stream().filter(filter::include)
        .forEach(region -> results.addRegionResult(getRegionResult(region)));
    return results;
  }

  RestoreRedundancyResults doRestoreRedundancy(PartitionedRegion region) {
    try {
      PartitionedRegionRebalanceOp op = getPartitionedRegionRebalanceOp(region);

      cache.getCancelCriterion().checkCancelInProgress(null);

      Set<PartitionRebalanceInfo> detailSet;
      try {
        detailSet = op.execute();

        RestoreRedundancyResultsImpl results = getEmptyRestoreRedundancyResults();
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
    RestoreRedundancyResultsImpl finalResult = getEmptyRestoreRedundancyResults();
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
    CompositeDirector director = new CompositeDirector(true, true, false, shouldReassign);
    director.setIsRestoreRedundancy(true);
    return new PartitionedRegionRebalanceOp(region, false, director, true, false,
        new AtomicBoolean(), manager.getStats());
  }

  // Extracted for testing
  RestoreRedundancyResultsImpl getEmptyRestoreRedundancyResults() {
    return new RestoreRedundancyResultsImpl();
  }

  // Extracted for testing
  RegionRedundancyStatus getRegionResult(PartitionedRegion region) {
    return new RegionRedundancyStatusImpl(region);
  }

  // Extracted for testing
  CompletableFuture<RestoreRedundancyResults> getResultsFuture(
      List<CompletableFuture<RestoreRedundancyResults>> regionFutures,
      CompletableFuture<Void> combinedFuture) {
    return combinedFuture.thenApplyAsync(voidd -> getRestoreRedundancyResults(regionFutures),
        executor);
  }
}
