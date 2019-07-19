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
package org.apache.geode.management.operation;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.geode.management.api.ClusterManagementOperation;
import org.apache.geode.management.runtime.RebalanceResult;

public class RebalanceOperation implements ClusterManagementOperation<RebalanceResult> {
  public static final String REBALANCE_ENDPOINT = "/operations/rebalance";
  private List<String> includeRegions = new ArrayList<>();
  private List<String> excludeRegions = new ArrayList<>();

  public List<String> getIncludeRegions() {
    return includeRegions;
  }

  public void setIncludeRegions(List<String> includeRegions) {
    this.includeRegions = includeRegions;
  }

  public List<String> getExcludeRegions() {
    return excludeRegions;
  }

  public void setExcludeRegions(List<String> excludeRegions) {
    this.excludeRegions = excludeRegions;
  }

  @Override
  @JsonIgnore
  public String getEndpoint() {
    return REBALANCE_ENDPOINT;
  }
}
