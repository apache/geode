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
package org.apache.geode.management.api;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.management.runtime.OperationResult;

/**
 * returned from {@link ClusterManagementService#list(ClusterManagementOperation)}
 *
 * @param <V> the result type associated with the operation type being listed
 */
@Experimental
public class ClusterManagementListOperationsResult<A extends ClusterManagementOperation<V>, V extends OperationResult>
    extends ClusterManagementResult {
  // Override the mapper setting so that we always show result
  @JsonInclude
  @JsonProperty
  private final List<ClusterManagementOperationResult<A, V>> result;

  /**
   * for internal use only
   */
  @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
  public ClusterManagementListOperationsResult(
      @JsonProperty("result") List<ClusterManagementOperationResult<A, V>> result) {
    this.result = result;
  }


  /**
   * Returns the payload of the list call
   */
  public List<ClusterManagementOperationResult<A, V>> getResult() {
    return result;
  }
}
