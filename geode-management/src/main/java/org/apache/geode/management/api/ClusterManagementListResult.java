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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.management.configuration.AbstractConfiguration;
import org.apache.geode.management.runtime.RuntimeInfo;

/**
 *
 * @param <T> the type of the static config, e.g. RegionConfig
 * @param <R> the type of the corresponding runtime information, e.g. RuntimeRegionInfo
 */
@Experimental
public class ClusterManagementListResult<T extends AbstractConfiguration<R>, R extends RuntimeInfo>
    extends ClusterManagementResult {
  /**
   * for internal use only
   */
  public ClusterManagementListResult() {}

  /**
   * for internal use only
   */
  public ClusterManagementListResult(StatusCode statusCode, String message) {
    super(statusCode, message);
  }

  // Override the mapper setting so that we always show result
  @JsonInclude
  @JsonProperty
  private List<ConfigurationResult<T, R>> result = new ArrayList<>();

  /**
   * Returns the combined payload of the list call
   */
  public List<ConfigurationResult<T, R>> getResult() {
    return result;
  }

  /**
   * Returns only the static config portion of the results
   */
  @JsonIgnore
  public List<T> getConfigResult() {
    return result.stream().map(ConfigurationResult::getConfiguration).collect(Collectors.toList());
  }

  /**
   * Returns only the runtime information portion of the results
   */
  @JsonIgnore
  public List<R> getRuntimeResult() {
    return result.stream().flatMap(r -> r.getRuntimeInfo().stream()).collect(Collectors.toList());
  }

  /**
   * for internal use only
   */
  public void setResult(List<ConfigurationResult<T, R>> result) {
    this.result = result;
  }
}
