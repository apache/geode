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

import org.apache.geode.annotations.Experimental;
import org.apache.geode.management.api.ClusterManagementOperation;
import org.apache.geode.management.runtime.RebalanceResult;

/**
 * Defines a distributed system request to optimize bucket allocation across members.
 */
@Experimental
public class RebalanceOperation implements ClusterManagementOperation<RebalanceResult> {
  private static final long serialVersionUID = 328709777841108084L;

  /**
   * see {@link #getEndpoint()}
   */
  public static final String REBALANCE_ENDPOINT = "/operations/rebalances";
  private final List<String> includeRegions = new ArrayList<>();
  private final List<String> excludeRegions = new ArrayList<>();
  private boolean simulate;
  private String operator;

  /**
   * by default, requests all partitioned regions to be rebalanced
   */
  public RebalanceOperation() {}

  /**
   * copy constructor
   */
  public RebalanceOperation(RebalanceOperation other) {
    setExcludeRegions(other.getExcludeRegions());
    setIncludeRegions(other.getIncludeRegions());
    setSimulate(other.isSimulate());
    operator = other.getOperator();
  }

  /***
   * Returns true if a "dry run" only is requested
   */
  public boolean isSimulate() {
    return simulate;
  }

  /**
   * true requests a "dry run" (no actual buckets will be moved)
   * default is false
   */
  public void setSimulate(boolean simulate) {
    this.simulate = simulate;
  }

  /***
   * Returns the list of regions to be rebalanced (or an empty list for all-except-excluded)
   */
  public List<String> getIncludeRegions() {
    return includeRegions;
  }

  /**
   * requests rebalance of the specified region(s) only. When at least one region is specified, this
   * takes precedence over any excluded regions.
   */
  public void setIncludeRegions(List<String> includeRegions) {
    this.includeRegions.clear();
    if (includeRegions != null) {
      this.includeRegions.addAll(includeRegions);
    }
  }

  /***
   * Returns the list of regions NOT to be rebalanced (iff {@link #getIncludeRegions()} is empty)
   */
  public List<String> getExcludeRegions() {
    return excludeRegions;
  }

  /**
   * excludes specific regions from the rebalance, if {@link #getIncludeRegions()} is empty,
   * otherwise has no effect
   * default: no regions are excluded
   */
  public void setExcludeRegions(List<String> excludeRegions) {
    this.excludeRegions.clear();
    if (excludeRegions != null) {
      this.excludeRegions.addAll(excludeRegions);
    }
  }

  @Override
  @JsonIgnore
  public String getEndpoint() {
    return REBALANCE_ENDPOINT;
  }

  @Override
  public String getOperator() {
    return operator;
  }

  public void setOperator(String operator) {
    this.operator = operator;
  }
}
