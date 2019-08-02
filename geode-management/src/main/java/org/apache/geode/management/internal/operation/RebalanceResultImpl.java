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

package org.apache.geode.management.internal.operation;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.geode.management.runtime.RebalanceRegionResult;
import org.apache.geode.management.runtime.RebalanceResult;

public class RebalanceResultImpl implements RebalanceResult, InfoResult {
  private List<RebalanceRegionResult> rebalanceSummary = new ArrayList<>();

  @JsonIgnore
  private String statusMessage;

  public RebalanceResultImpl() {}

  @Override
  public List<RebalanceRegionResult> getRebalanceRegionResults() {
    return rebalanceSummary;
  }

  public void setRebalanceSummary(List<RebalanceRegionResult> rebalanceSummary) {
    this.rebalanceSummary = rebalanceSummary;
  }

  @Override
  public String getStatusMessage() {
    return statusMessage;
  }

  public void setStatusMessage(String statusMessage) {
    this.statusMessage = statusMessage;
  }

  @Override
  public String toString() {
    return "{" + rebalanceSummary.stream().map(Object::toString).collect(Collectors.joining(",\n "))
        + "}";
  }
}
