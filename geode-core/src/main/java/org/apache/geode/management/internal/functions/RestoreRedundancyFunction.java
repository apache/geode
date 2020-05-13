/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.management.internal.functions;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.control.RegionRedundancyStatus;
import org.apache.geode.cache.control.ResourceManager;
import org.apache.geode.cache.control.RestoreRedundancyOperation;
import org.apache.geode.cache.control.RestoreRedundancyResults;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.internal.operation.RegionRedundancyStatusSerializableImpl;
import org.apache.geode.management.internal.operation.RestoreRedundancyResponseImpl;
import org.apache.geode.management.operation.RestoreRedundancyRequest;
import org.apache.geode.management.runtime.RegionRedundancyStatusSerializable;


public class RestoreRedundancyFunction implements InternalFunction<RestoreRedundancyRequest> {
  private static final Logger logger = LogService.getLogger();

  public static final String ID = RestoreRedundancyFunction.class.getName();


  private static final long serialVersionUID = 1L;

  @Override
  public void execute(FunctionContext<RestoreRedundancyRequest> context) {

    Cache cache = context.getCache();
    ResourceManager manager = cache.getResourceManager();
    RestoreRedundancyRequest arg =  context.getArguments();

    RestoreRedundancyOperation operation = manager.createRestoreRedundancyOperation();
    operation.excludeRegions(new HashSet<>(arg.getExcludeRegions()));
    operation.includeRegions(new HashSet<>(arg.getIncludeRegions()));
    operation.shouldReassignPrimaries(arg.getReassignPrimaries());

    RestoreRedundancyResults results;

    try {
      results = operation.start().join();

      RestoreRedundancyResponseImpl restoreRedundancyResponse = new RestoreRedundancyResponseImpl();

      buildList(results.getSatisfiedRedundancyRegionResults().entrySet(), restoreRedundancyResponse.getSatisfiedRedundancyRegionResults());
      buildList(results.getUnderRedundancyRegionResults().entrySet(), restoreRedundancyResponse.getUnderRedundancyRegionResults());
      buildList(results.getZeroRedundancyRegionResults().entrySet(), restoreRedundancyResponse.getZeroRedundancyRegionResults());

      restoreRedundancyResponse.setStatusMessage(results.getMessage());
      restoreRedundancyResponse.setSuccess(results.getStatus() == RestoreRedundancyResults.Status.SUCCESS);
      restoreRedundancyResponse.setTotalPrimaryTransferTime(results.getTotalPrimaryTransferTime().toMillis());
      restoreRedundancyResponse.setTotalPrimaryTransfersCompleted(results.getTotalPrimaryTransfersCompleted());

      context.getResultSender().lastResult(restoreRedundancyResponse);

    } catch (CancellationException e) {
      logger.info("Starting RestoreRedundancyFunction CancellationException: {}", e.getMessage(), e);
      context.getResultSender().lastResult("CancellationException1 " + e.getMessage());
    }
  }
  private void buildList(Set<Map.Entry<String, RegionRedundancyStatus>> entrySet,
                         List<RegionRedundancyStatusSerializable> listOfResults) {
    logger.info("MLH buildList entered");
    for (Map.Entry<String, RegionRedundancyStatus> entry : entrySet) {
      RegionRedundancyStatus value = entry.getValue();
      logger.info("MLH buildList value = " + value);
      RegionRedundancyStatusSerializableImpl regionRedundancyStatusSerializable =
          new RegionRedundancyStatusSerializableImpl();

      regionRedundancyStatusSerializable.setRegionName(value.getRegionName());
      regionRedundancyStatusSerializable.setConfiguredRedundancy(value.getConfiguredRedundancy());
      regionRedundancyStatusSerializable.setActualRedundancy(value.getActualRedundancy());
      regionRedundancyStatusSerializable.setStatus(RegionRedundancyStatusSerializable.RedundancyStatus.values()[value.getStatus().ordinal()]);
      listOfResults.add(regionRedundancyStatusSerializable);
    }
    logger.info("MLH buildList finished");
  }

  @Override
  public String getId() {
    return RestoreRedundancyFunction.ID;
  }

  @Override
  public boolean hasResult() {
    return true;
  }

  @Override
  public boolean optimizeForWrite() {
    // no need of optimization since read-only.
    return false;
  }

  @Override
  public boolean isHA() {
    return false;
  }

}
