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
import org.apache.geode.cache.configuration.EnumActionDestroyOverflow;
import org.apache.geode.cache.configuration.RegionAttributesDataPolicy;
import org.apache.geode.cache.configuration.RegionAttributesScope;
import org.apache.geode.cache.configuration.RegionAttributesType;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.cache.configuration.RegionType;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.RegionNameValidation;
import org.apache.geode.management.internal.CacheElementOperation;
import org.apache.geode.security.ResourcePermission;

public class RegionConfigValidator implements ConfigurationValidator<RegionConfig> {
  private InternalCache cache;

  public RegionConfigValidator(InternalCache cache) {
    this.cache = cache;
  }

  @Override
  public void validate(CacheElementOperation operation, RegionConfig config)
      throws IllegalArgumentException {
    switch (operation) {
      case UPDATE:
      case CREATE:
        validateCreate(config);
        break;
      case DELETE:
      default:
    }
  }

  private void validateCreate(RegionConfig config) {
    if (config.getType() == null) {
      throw new IllegalArgumentException("Type of the region has to be specified.");
    }

    // validate if the type is a valid RegionType. Only types defined in RegionType are supported
    // by management v2 api.
    try {
      RegionType.valueOf(config.getType());
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          String.format("Type %s is not supported in Management V2 API.", config.getType()));
    }

    RegionNameValidation.validate(config.getName());

    setShortcutAttributes(config);

    // additional authorization
    if (config.getRegionAttributes().getDataPolicy().isPersistent()) {
      cache.getSecurityService()
          .authorize(ResourcePermission.Resource.CLUSTER, ResourcePermission.Operation.WRITE,
              ResourcePermission.Target.DISK);
    }
  }

  public static void setShortcutAttributes(RegionConfig config) {
    String type = config.getType();
    RegionAttributesType regionAttributes;

    if (config.getRegionAttributes() == null) {
      regionAttributes = new RegionAttributesType();
      config.setRegionAttributes(regionAttributes);
    }

    regionAttributes = config.getRegionAttributes();
    switch (type) {
      case "PARTITION": {
        checkAndSetDataPolicy(RegionAttributesDataPolicy.PARTITION, regionAttributes);
        break;
      }
      case "REPLICATE": {
        checkAndSetDataPolicy(RegionAttributesDataPolicy.REPLICATE, regionAttributes);
        checkAndSetScope(RegionAttributesScope.DISTRIBUTED_ACK, regionAttributes);
        break;
      }
      case "PARTITION_REDUNDANT": {
        checkAndSetDataPolicy(RegionAttributesDataPolicy.PARTITION, regionAttributes);
        checkAndSetRedundancyCopy("1", regionAttributes);
        break;
      }
      case "PARTITION_PERSISTENT": {
        checkAndSetDataPolicy(RegionAttributesDataPolicy.PERSISTENT_PARTITION, regionAttributes);
        break;
      }
      case "PARTITION_REDUNDANT_PERSISTENT": {
        checkAndSetDataPolicy(RegionAttributesDataPolicy.PERSISTENT_PARTITION, regionAttributes);
        checkAndSetRedundancyCopy("1", regionAttributes);
        break;
      }
      case "PARTITION_OVERFLOW": {
        checkAndSetDataPolicy(RegionAttributesDataPolicy.PARTITION, regionAttributes);
        checkAndSetEvictionAction(EnumActionDestroyOverflow.OVERFLOW_TO_DISK, regionAttributes);
        break;
      }
      case "PARTITION_REDUNDANT_OVERFLOW": {
        checkAndSetDataPolicy(RegionAttributesDataPolicy.PARTITION, regionAttributes);
        checkAndSetRedundancyCopy("1", regionAttributes);
        checkAndSetEvictionAction(EnumActionDestroyOverflow.OVERFLOW_TO_DISK, regionAttributes);
        break;
      }
      case "PARTITION_PERSISTENT_OVERFLOW": {
        checkAndSetDataPolicy(RegionAttributesDataPolicy.PERSISTENT_PARTITION, regionAttributes);
        checkAndSetEvictionAction(EnumActionDestroyOverflow.OVERFLOW_TO_DISK, regionAttributes);
        break;
      }
      case "PARTITION_REDUNDANT_PERSISTENT_OVERFLOW": {
        checkAndSetDataPolicy(RegionAttributesDataPolicy.PERSISTENT_PARTITION, regionAttributes);
        checkAndSetRedundancyCopy("1", regionAttributes);
        checkAndSetEvictionAction(EnumActionDestroyOverflow.OVERFLOW_TO_DISK, regionAttributes);
        break;
      }
      case "PARTITION_HEAP_LRU": {
        checkAndSetDataPolicy(RegionAttributesDataPolicy.PARTITION, regionAttributes);
        checkAndSetEvictionAction(EnumActionDestroyOverflow.LOCAL_DESTROY, regionAttributes);
        break;

      }
      case "PARTITION_REDUNDANT_HEAP_LRU": {
        checkAndSetDataPolicy(RegionAttributesDataPolicy.PARTITION, regionAttributes);
        checkAndSetRedundancyCopy("1", regionAttributes);
        checkAndSetEvictionAction(EnumActionDestroyOverflow.LOCAL_DESTROY, regionAttributes);
        break;
      }

      case "REPLICATE_PERSISTENT": {
        checkAndSetDataPolicy(RegionAttributesDataPolicy.PERSISTENT_REPLICATE, regionAttributes);
        checkAndSetScope(RegionAttributesScope.DISTRIBUTED_ACK, regionAttributes);
        break;
      }
      case "REPLICATE_OVERFLOW": {
        checkAndSetDataPolicy(RegionAttributesDataPolicy.REPLICATE, regionAttributes);
        checkAndSetScope(RegionAttributesScope.DISTRIBUTED_ACK, regionAttributes);
        checkAndSetEvictionAction(EnumActionDestroyOverflow.OVERFLOW_TO_DISK, regionAttributes);
        break;

      }
      case "REPLICATE_PERSISTENT_OVERFLOW": {
        checkAndSetDataPolicy(RegionAttributesDataPolicy.PERSISTENT_REPLICATE, regionAttributes);
        checkAndSetScope(RegionAttributesScope.DISTRIBUTED_ACK, regionAttributes);
        checkAndSetEvictionAction(EnumActionDestroyOverflow.OVERFLOW_TO_DISK, regionAttributes);
        break;
      }
      case "REPLICATE_HEAP_LRU": {
        checkAndSetDataPolicy(RegionAttributesDataPolicy.PRELOADED, regionAttributes);
        checkAndSetScope(RegionAttributesScope.DISTRIBUTED_ACK, regionAttributes);
        regionAttributes.setInterestPolicy("all");
        checkAndSetEvictionAction(EnumActionDestroyOverflow.LOCAL_DESTROY, regionAttributes);
        break;
      }
      case "LOCAL": {
        checkAndSetDataPolicy(RegionAttributesDataPolicy.NORMAL, regionAttributes);
        checkAndSetScope(RegionAttributesScope.LOCAL, regionAttributes);
        break;
      }
      case "LOCAL_PERSISTENT": {
        checkAndSetDataPolicy(RegionAttributesDataPolicy.PERSISTENT_REPLICATE, regionAttributes);
        checkAndSetScope(RegionAttributesScope.LOCAL, regionAttributes);
        break;
      }
      case "LOCAL_HEAP_LRU": {
        checkAndSetDataPolicy(RegionAttributesDataPolicy.NORMAL, regionAttributes);
        checkAndSetScope(RegionAttributesScope.LOCAL, regionAttributes);
        checkAndSetEvictionAction(EnumActionDestroyOverflow.LOCAL_DESTROY, regionAttributes);
        break;
      }
      case "LOCAL_OVERFLOW": {
        checkAndSetDataPolicy(RegionAttributesDataPolicy.NORMAL, regionAttributes);
        checkAndSetScope(RegionAttributesScope.LOCAL, regionAttributes);
        checkAndSetEvictionAction(EnumActionDestroyOverflow.OVERFLOW_TO_DISK, regionAttributes);
        break;
      }
      case "LOCAL_PERSISTENT_OVERFLOW": {
        checkAndSetDataPolicy(RegionAttributesDataPolicy.PERSISTENT_REPLICATE, regionAttributes);
        checkAndSetScope(RegionAttributesScope.LOCAL, regionAttributes);
        checkAndSetEvictionAction(EnumActionDestroyOverflow.OVERFLOW_TO_DISK, regionAttributes);
        break;
      }
      case "PARTITION_PROXY": {
        checkAndSetDataPolicy(RegionAttributesDataPolicy.PARTITION, regionAttributes);
        checkAndSetLocalMaxMemory("0", regionAttributes);
        break;
      }
      case "PARTITION_PROXY_REDUNDANT": {
        checkAndSetDataPolicy(RegionAttributesDataPolicy.PARTITION, regionAttributes);
        checkAndSetLocalMaxMemory("0", regionAttributes);
        checkAndSetRedundancyCopy("1", regionAttributes);
        break;
      }
      case "REPLICATE_PROXY": {
        checkAndSetDataPolicy(RegionAttributesDataPolicy.EMPTY, regionAttributes);
        checkAndSetScope(RegionAttributesScope.DISTRIBUTED_ACK, regionAttributes);
        break;
      }
      default:
        throw new IllegalArgumentException("invalid type " + type);
    }
  }

  private static void checkAndSetLocalMaxMemory(String maxMemory,
      RegionAttributesType regionAttributes) {
    if (regionAttributes.getPartitionAttributes() == null
        || regionAttributes.getPartitionAttributes().getLocalMaxMemory() == null) {
      regionAttributes.setLocalMaxMemory(maxMemory);
    }
    String existing = regionAttributes.getPartitionAttributes().getLocalMaxMemory();
    if (!existing.equals(maxMemory)) {
      throw new IllegalArgumentException("Invalid local max memory: " + existing);
    }
  }

  private static void checkAndSetEvictionAction(EnumActionDestroyOverflow evictionAction,
      RegionAttributesType regionAttributes) {
    if (regionAttributes.getEvictionAttributes() == null
        || regionAttributes.getEvictionAttributes().getLruHeapPercentage() == null
        || regionAttributes.getEvictionAttributes().getLruHeapPercentage().getAction() == null) {
      regionAttributes.setLruHeapPercentageEvictionAction(evictionAction);
    }

    EnumActionDestroyOverflow existing =
        regionAttributes.getEvictionAttributes().getLruHeapPercentage().getAction();
    if (existing != evictionAction) {
      throw new IllegalArgumentException("Conflicting eviction action " + existing.toString());
    }
  }

  private static void checkAndSetScope(RegionAttributesScope scope,
      RegionAttributesType regionAttributes) {
    RegionAttributesScope existing = regionAttributes.getScope();
    if (existing == null) {
      regionAttributes.setScope(scope);
    } else if (existing != scope) {
      throw new IllegalArgumentException("Conflicting scope " + existing.toString());
    }
  }

  private static void checkAndSetDataPolicy(RegionAttributesDataPolicy policy,
      RegionAttributesType regionAttributes) {
    RegionAttributesDataPolicy existing = regionAttributes.getDataPolicy();
    if (existing == null) {
      regionAttributes.setDataPolicy(policy);
    } else if (existing != policy) {
      throw new IllegalArgumentException("Conflicting data policy "
          + existing.toString());
    }
  }

  // need to do this if user already set the redundant copy in the RegionAttributeType
  private static void checkAndSetRedundancyCopy(String copies,
      RegionAttributesType regionAttributes) {
    if (regionAttributes.getPartitionAttributes() == null
        || regionAttributes.getPartitionAttributes().getRedundantCopies() == null) {
      regionAttributes.setRedundantCopy(copies);
    }
    RegionAttributesType.PartitionAttributes partitionAttributes =
        regionAttributes.getPartitionAttributes();
    if ("0".equals(partitionAttributes.getRedundantCopies())) {
      throw new IllegalArgumentException(
          "Conflicting redundant copy when region type is REDUNDANT");
    }
  }

  @Override
  public boolean exists(String id, CacheConfig existing) {
    return CacheElement.exists(existing.getRegions(), id);
  }
}
