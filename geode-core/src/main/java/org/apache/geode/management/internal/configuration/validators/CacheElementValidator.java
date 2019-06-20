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

import org.apache.commons.lang3.StringUtils;

import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.management.internal.CacheElementOperation;

/**
 * this is used to validate all the common attributes of CacheElement, eg. name and group
 */
public class CacheElementValidator implements ConfigurationValidator<CacheElement> {
  @Override
  public void validate(CacheElementOperation operation, CacheElement config)
      throws IllegalArgumentException {
    if (StringUtils.isBlank(config.getId())) {
      throw new IllegalArgumentException("id cannot be null or blank");
    }

    switch (operation) {
      case UPDATE:
      case CREATE:
        validateCreate(config);
        break;
      case DELETE:
        break;
      default:
    }
  }

  private void validateCreate(CacheElement config) {
    String group = config.getGroup();
    if (CacheElement.CLUSTER.equalsIgnoreCase(group)) {
      throw new IllegalArgumentException("'"
          + CacheElement.CLUSTER
          + "' is a reserved group name. Do not use it for member groups.");
    }
    if (group != null && group.contains(",")) {
      throw new IllegalArgumentException("Group name should not contain comma.");
    }
    String id = config.getId();
    if (id.contains("/")) {
      throw new IllegalArgumentException("Id should not contain slash.");
    }
  }

}
