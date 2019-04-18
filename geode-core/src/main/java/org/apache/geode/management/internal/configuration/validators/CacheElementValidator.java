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

package org.apache.geode.management.internal.configuration.validators;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheElement;

/**
 * this is used to validate all the common attributes of CacheElement, eg. group
 */
public class CacheElementValidator implements ConfigurationValidator<CacheElement> {
  @Override
  public void validate(CacheElement config) throws IllegalArgumentException {
    if ("cluster".equalsIgnoreCase(config.getGroup())) {
      throw new IllegalArgumentException(
          "cluster is a reserved group name. Do not use it for member groups.");
    }
  }

  @Override
  public boolean exists(CacheElement config, CacheConfig persistedConfig) {
    return false;
  }
}
