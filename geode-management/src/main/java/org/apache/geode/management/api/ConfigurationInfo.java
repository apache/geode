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

import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.geode.management.configuration.AbstractConfiguration;
import org.apache.geode.management.runtime.RuntimeInfo;

public class ConfigurationInfo<T extends AbstractConfiguration<R>, R extends RuntimeInfo> {
  private String id;
  @JsonInclude
  @JsonProperty
  private List<ConfigurationResult<T, R>> groupResults = new ArrayList<>();

  public ConfigurationInfo() {}

  public ConfigurationInfo(String id, List<ConfigurationResult<T, R>> groupResults) {
    this.id = id;
    this.groupResults = groupResults;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public List<ConfigurationResult<T, R>> getGroupResults() {
    return groupResults;
  }

  public void setGroupResults(List<ConfigurationResult<T, R>> groupResults) {
    this.groupResults = groupResults;
  }

  @JsonIgnore
  public List<T> getConfigurations() {
    return groupResults.stream()
        .map(ConfigurationResult::getConfiguration)
        .collect(toList());
  }

  @JsonIgnore
  public List<R> getRuntimeInfos() {
    return groupResults.stream()
        .flatMap(r -> r.getRuntimeInfo().stream())
        .collect(toList());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ConfigurationInfo<?, ?> that = (ConfigurationInfo<?, ?>) o;
    return Objects.equals(id, that.id) &&
        Objects.equals(groupResults, that.groupResults);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, groupResults);
  }

  @Override
  public String toString() {
    return "ConfigurationInfo{" +
        "id='" + id + '\'' +
        ", groupResults=" + groupResults +
        '}';
  }
}
