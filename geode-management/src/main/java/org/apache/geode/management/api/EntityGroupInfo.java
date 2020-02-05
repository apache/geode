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

import static java.util.Collections.emptyList;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.management.configuration.AbstractConfiguration;
import org.apache.geode.management.configuration.Links;
import org.apache.geode.management.runtime.RuntimeInfo;

/**
 * A simple object that holds a configuration object and its corresponding runtime info on each
 * member.
 *
 * This represents the configuration per group and it's corresponding member runtimeInfo
 *
 * @param <T> the config type
 * @param <R> the runtimeInfo type
 */
@Experimental
public class EntityGroupInfo<T extends AbstractConfiguration<R>, R extends RuntimeInfo> {
  private T configuration;
  private List<R> runtimeInfo = new ArrayList<>();

  /**
   * for internal use only
   */
  public EntityGroupInfo() {}

  /**
   * for internal use only
   */
  public EntityGroupInfo(T configuration) {
    this(configuration, emptyList());
  }

  public EntityGroupInfo(T configuration, List<R> runtimeInfo) {
    this.configuration = configuration;
    this.runtimeInfo = runtimeInfo;
  }

  /**
   * Returns the static portion of the configuration
   */
  public T getConfiguration() {
    return configuration;
  }

  /**
   * for internal use only
   */
  public void setConfiguration(T configuration) {
    this.configuration = configuration;
  }

  /**
   * Returns the non-static information
   */
  public List<R> getRuntimeInfo() {
    return runtimeInfo;
  }

  /**
   * for internal use only
   */
  public void setRuntimeInfo(List<R> runtimeInfo) {
    this.runtimeInfo = runtimeInfo;
  }

  @JsonIgnore
  public Links getLinks() {
    return configuration.getLinks();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    EntityGroupInfo<?, ?> that = (EntityGroupInfo<?, ?>) o;
    return Objects.equals(configuration, that.configuration) &&
        Objects.equals(runtimeInfo, that.runtimeInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hash(configuration, runtimeInfo);
  }

  @Override
  public String toString() {
    return "ConfigurationResult{" +
        "configuration=" + configuration +
        ", runtimeInfo=" + runtimeInfo +
        '}';
  }
}
