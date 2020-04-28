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

import static org.apache.geode.cache.control.RestoreRedundancyResults.Status.ERROR;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.geode.cache.control.RestoreRedundancyOperation;
import org.apache.geode.cache.control.RestoreRedundancyResults;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.management.cli.CliFunction;
import org.apache.geode.management.internal.functions.CliFunctionResult;

public class RedundancyCommandFunction extends CliFunction<Object[]> {
  private static final long serialVersionUID = 5633636343813884996L;

  @Override
  public CliFunctionResult executeFunction(FunctionContext<Object[]> context) {
    Object[] arguments = context.getArguments();
    String[] includeRegions = (String[]) arguments[0];
    Set<String> includeRegionsSet = null;
    if (includeRegions != null) {
      includeRegionsSet = new HashSet<>(Arrays.asList(includeRegions));
    }

    String[] excludeRegions = (String[]) arguments[1];
    Set<String> excludeRegionsSet = null;
    if (excludeRegions != null) {
      excludeRegionsSet = new HashSet<>(Arrays.asList(excludeRegions));
    }

    boolean shouldReassignPrimaries = (boolean) arguments[2];

    boolean isStatusCommand = false;
    if (arguments.length > 3) {
      isStatusCommand = (boolean) arguments[3];
    }

    RestoreRedundancyResults results;
    RestoreRedundancyOperation redundancyOperation =
        context.getCache().getResourceManager().createRestoreRedundancyOperation();
    redundancyOperation.includeRegions(includeRegionsSet);
    redundancyOperation.excludeRegions(excludeRegionsSet);
    if (isStatusCommand) {
      results = redundancyOperation.redundancyStatus();
    } else {
      redundancyOperation.shouldReassignPrimaries(shouldReassignPrimaries);
      results = redundancyOperation.start().join();
    }

    if (results.getStatus().equals(ERROR)) {
      return new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.ERROR,
          results.getMessage());
    }

    return new CliFunctionResult(context.getMemberName(), results);
  }
}
