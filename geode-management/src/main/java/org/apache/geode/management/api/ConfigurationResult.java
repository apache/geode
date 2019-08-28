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

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.management.runtime.RuntimeInfo;

/**
 * A simple object that holds a configuration object and its corresponding runtime info on each
 * member.
 *
 * @param <T> the config type
 * @param <R> the runtimeInfo type
 */
@Experimental
public class ConfigurationResult<T extends CacheElement & CorrespondWith<R>, R extends RuntimeInfo> {
  private T configuration;
  private List<R> runtimeInfo = new ArrayList<>();

  /**
   * for internal use only
   */
  public ConfigurationResult() {}

  /**
   * for internal use only
   */
  public ConfigurationResult(T configuration) {
    this.configuration = configuration;
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
}
