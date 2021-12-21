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


import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.RegionNameValidation;
import org.apache.geode.management.configuration.AbstractConfiguration;
import org.apache.geode.management.configuration.Region;
import org.apache.geode.management.configuration.RegionType;
import org.apache.geode.management.internal.CacheElementOperation;
import org.apache.geode.security.ResourcePermission;

public class RegionConfigValidator implements ConfigurationValidator<Region> {
  private final InternalCache cache;

  public RegionConfigValidator(InternalCache cache) {
    this.cache = cache;
  }

  @Override
  public void validate(CacheElementOperation operation, Region config)
      throws IllegalArgumentException {
    switch (operation) {
      case UPDATE:
      case CREATE:
        validateCreate(config);
        break;
      case DELETE:
        validateDelete(config);
      default:
    }
  }

  private void validateDelete(AbstractConfiguration config) {
    if (StringUtils.isNotBlank(config.getGroup())) {
      throw new IllegalArgumentException(
          "Group is an invalid option when deleting region.");
    }
  }

  private void validateCreate(Region config) {
    if (config.getType() == null) {
      throw new IllegalArgumentException("Region type is required.");
    }

    if (config.getType() == RegionType.LEGACY) {
      throw new IllegalArgumentException(("Region type is unsupported."));
    }

    Integer redundantCopies = config.getRedundantCopies();
    if (redundantCopies != null && (redundantCopies < 0 || redundantCopies > 3)) {
      throw new IllegalArgumentException(
          "redundantCopies cannot be less than 0 or greater than 3.");
    }

    if (!config.getType().withPartition() && config.getRedundantCopies() != null) {
      throw new IllegalArgumentException("redundantCopies can only be set with PARTITION regions.");
    }

    RegionNameValidation.validate(config.getName());

    // additional authorization
    if (config.getType().name().contains("PERSISTENT")) {
      cache.getSecurityService()
          .authorize(ResourcePermission.Resource.CLUSTER, ResourcePermission.Operation.WRITE,
              ResourcePermission.Target.DISK);
    }

    // validate expirations
    List<Region.Expiration> expirations = config.getExpirations();
    if (expirations != null) {
      Set<Region.ExpirationType> existingTypes = new HashSet<>();
      for (Region.Expiration expiration : expirations) {
        validate(expiration);
        if (existingTypes.contains(expiration.getType())) {
          throw new IllegalArgumentException("Can not have multiple " + expiration.getType() + ".");
        }
        existingTypes.add(expiration.getType());
      }
    }

    // validate eviction
    validateEviction(config.getEviction());
  }

  private void validateEviction(Region.Eviction eviction) {
    if (eviction == null) {
      return;
    }

    Region.EvictionType type = eviction.getType();
    if (type == null) {
      throw new IllegalArgumentException("Eviction type must be set.");
    }

    switch (type) {
      case ENTRY_COUNT:
        if (eviction.getEntryCount() == null) {
          throw new IllegalArgumentException(
              "EntryCount must be set for: " + type);
        }
        if (eviction.getObjectSizer() != null) {
          throw new IllegalArgumentException(
              "ObjectSizer must not be set for: " + type);
        }
        break;
      case MEMORY_SIZE:
        if (eviction.getMemorySizeMb() == null) {
          throw new IllegalArgumentException(
              "MemorySizeMb must be set for: " + type);
        }
        break;
    }
  }

  private void validate(Region.Expiration expiration) {
    if (expiration.getType() == null) {
      throw new IllegalArgumentException("Expiration type must be set.");
    }

    if (expiration.getType() == Region.ExpirationType.LEGACY) {
      throw new IllegalArgumentException("Invalid Expiration type.");
    }

    if (expiration.getTimeInSeconds() == null || expiration.getTimeInSeconds() < 0) {
      throw new IllegalArgumentException(
          ("Expiration timeInSeconds must be greater than or equal to 0."));
    }

    if (expiration.getAction() == Region.ExpirationAction.LEGACY) {
      throw new IllegalArgumentException("Invalid Expiration action.");
    }
  }
}
