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
import org.apache.geode.cache.configuration.ManagedRegionConfig;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.cache.configuration.RegionType;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.RegionNameValidation;
import org.apache.geode.security.ResourcePermission;

public class RegionConfigValidator implements ConfigurationValidator<ManagedRegionConfig> {
  private InternalCache cache;

  public RegionConfigValidator(InternalCache cache) {
    this.cache = cache;
  }

  @Override
  public void validate(ManagedRegionConfig config)
      throws IllegalArgumentException {
    if (config instanceof RegionConfig) {
      throw new IllegalArgumentException("Use ManagedRegionConfig to configure your region.");
    }

    if (config.getName() == null) {
      throw new IllegalArgumentException("Name of the region has to be specified.");
    }

    RegionNameValidation.validate(config.getName());

    if ("cluster".equalsIgnoreCase(config.getGroup())) {
      throw new IllegalArgumentException(
          "cluster is a reserved group name. Do not use it for member groups.");
    }

    if (config.getType() == null) {
      RegionType defaultRegion = RegionType.PARTITION;
      config.setType(defaultRegion);
    } else {
      String type = config.getType();
      // validate if the type is a valid RegionType. Only types defined in RegionType are supported
      // by management v2 api.
      try {
        RegionType.valueOf(type);

      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            String.format("Type %s is not supported in Management V2 API.", type));
      }

      // additional authorization
      if (config.getRegionAttributes().getDataPolicy().isPersistent()) {
        cache.getSecurityService()
            .authorize(ResourcePermission.Resource.CLUSTER, ResourcePermission.Operation.WRITE,
                ResourcePermission.Target.DISK);
      }
    }

  }

  @Override
  public boolean exists(ManagedRegionConfig config, CacheConfig existing) {
    return CacheElement.exists(existing.getRegions(), config.getId());
  }
}
