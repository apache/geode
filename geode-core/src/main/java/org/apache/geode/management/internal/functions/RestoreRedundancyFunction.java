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
package org.apache.geode.management.internal.functions;

import java.util.HashSet;
import java.util.Set;

import org.apache.geode.cache.control.RestoreRedundancyOperation;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.cache.control.SerializableRestoreRedundancyResultsImpl;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.management.internal.operation.RestoreRedundancyResultsImpl;
import org.apache.geode.management.operation.RestoreRedundancyRequest;


public class RestoreRedundancyFunction implements InternalFunction<Object[]> {

  public static final String ID = RestoreRedundancyFunction.class.getName();
  private static final long serialVersionUID = -8991672237560920252L;


  @Override
  // this would return the RestoreRedundancyResults if successful,
  // it will return an exception to the caller if status is failure or any exception happens
  public void execute(FunctionContext<Object[]> context) {
    Object[] arguments = context.getArguments();
    RestoreRedundancyRequest request = (RestoreRedundancyRequest) arguments[0];
    boolean isStatusCommand = (boolean) arguments[1];
    RestoreRedundancyOperation redundancyOperation =
        context.getCache().getResourceManager().createRestoreRedundancyOperation();
    Set<String> includeRegionsSet = null;
    if (request.getIncludeRegions() != null) {
      includeRegionsSet = new HashSet<>(request.getIncludeRegions());
    }
    Set<String> excludeRegionsSet = null;
    if (request.getExcludeRegions() != null) {
      excludeRegionsSet = new HashSet<>(request.getExcludeRegions());
    }
    redundancyOperation.includeRegions(includeRegionsSet);
    redundancyOperation.excludeRegions(excludeRegionsSet);
    RestoreRedundancyResultsImpl results;

    try {
      if (isStatusCommand) {
        results = (RestoreRedundancyResultsImpl) redundancyOperation.redundancyStatus();
      } else {
        redundancyOperation.shouldReassignPrimaries(request.getReassignPrimaries());
        results = (RestoreRedundancyResultsImpl) redundancyOperation.start().join();
      }
      results.setSuccess(true);
      results.setStatusMessage("Success");
    } catch (Exception e) {
      results = new SerializableRestoreRedundancyResultsImpl();
      results.setSuccess(false);
      results.setStatusMessage(e.getMessage());
    }
    context.getResultSender().lastResult(results);
  }

  @Override
  public String getId() {
    return ID;
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
