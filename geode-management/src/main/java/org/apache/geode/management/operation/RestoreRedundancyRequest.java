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

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.management.api.ClusterManagementOperation;
import org.apache.geode.management.runtime.RestoreRedundancyResults;

/**
 * Defines a distributed system request to optimize bucket allocation across members.
 * RestoreRedundancyRequest - is part of an experimental API for performing operations on GEODE
 * using a REST interface. This API is experimental and may change.
 *
 */

@Experimental
public class RestoreRedundancyRequest
    implements ClusterManagementOperation<RestoreRedundancyResults> {

  /**
   * see {@link #getEndpoint()}
   */
  public static final String RESTORE_REDUNDANCY_ENDPOINT = "/operations/restoreRedundancy";
  private static final long serialVersionUID = -3896185413062876188L;
  /** null means all regions included */
  private List<String> includeRegions;
  /** null means don't exclude any regions */
  private List<String> excludeRegions;
  private boolean reassignPrimaries = true;
  private String operator;

  /**
   * By default, requests all partitioned regions to have redundancy restored
   */
  public RestoreRedundancyRequest() {}

  /**
   * Copy constructor
   *
   * @param other the {@link RestoreRedundancyRequest} to be copied
   */
  public RestoreRedundancyRequest(
      RestoreRedundancyRequest other) {
    setExcludeRegions(other.getExcludeRegions());
    setIncludeRegions(other.getIncludeRegions());
    setReassignPrimaries(other.getReassignPrimaries());
    operator = other.getOperator();
  }

  /**
   * Returns the list of regions to have redundancy restored (or an empty list for
   * all-except-excluded)
   *
   * @return a list of regions to have redundancy restored (or an empty list for
   *         all-except-excluded)
   */
  public List<String> getIncludeRegions() {
    return includeRegions;
  }

  /**
   * Requests restore redundancy of the specified region(s) only. When at least one region is
   * specified, this takes precedence over any excluded regions.
   *
   * @param includeRegions a list of region names to include in the restore redundancy operation
   */
  public void setIncludeRegions(List<String> includeRegions) {
    this.includeRegions = includeRegions;
  }

  /**
   * Returns the list of regions NOT to have redundancy restored (iff {@link #getIncludeRegions()}
   * is empty)
   *
   * @return the list of regions NOT to have redundancy restored (iff {@link #getIncludeRegions()}
   *         is empty
   */
  public List<String> getExcludeRegions() {
    return excludeRegions;
  }

  /**
   * Excludes specific regions from the restore redundancy, if {@link #getIncludeRegions()} is
   * empty,
   * otherwise has no effect
   * default: no regions are excluded
   *
   * @param excludeRegions a list of region names to exclude from the restore redundancy operation
   */
  public void setExcludeRegions(List<String> excludeRegions) {
    this.excludeRegions = excludeRegions;
  }

  public void setReassignPrimaries(boolean reassignPrimaries) {
    this.reassignPrimaries = reassignPrimaries;
  }

  public boolean getReassignPrimaries() {
    return reassignPrimaries;
  }

  @Override
  @JsonIgnore
  public String getEndpoint() {
    return RESTORE_REDUNDANCY_ENDPOINT;
  }

  @Override
  public String getOperator() {
    return operator;
  }

  public void setOperator(String operator) {
    this.operator = operator;
  }

  @Override
  public String toString() {
    return "RestoreRedundancyRequest{" +
        "includeRegions=" + includeRegions +
        ", excludeRegions=" + excludeRegions +
        ", reassignPrimaries=" + reassignPrimaries +
        ", operator='" + operator + '\'' +
        '}';
  }
}
