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
package org.apache.geode.modules.util;

import static java.util.Collections.singletonList;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.function.BiFunction;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializable;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.internal.locks.DistributedMemberLock;
import org.apache.geode.internal.cache.CacheFactoryStatics;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegionFactory;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.xmlcache.CacheXmlGenerator;
import org.apache.geode.internal.cache.xmlcache.RegionAttributesCreation;
import org.apache.geode.internal.util.TriConsumer;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.internal.security.ResourcePermissions;
import org.apache.geode.security.ResourcePermission;

public class CreateRegionFunction implements Function, Declarable, DataSerializable {
  private static final Logger logger = LogService.getLogger();

  public static final String ID = "create-region-function";

  @VisibleForTesting
  static final String REGION_CONFIGURATION_METADATA_REGION = "__regionConfigurationMetadata";

  private static final boolean DUMP_SESSION_CACHE_XML =
      Boolean.getBoolean("gemfiremodules.dumpSessionCacheXml");

  private final InternalCache cache;
  private final Region<String, RegionConfiguration> regionConfigurationsRegion;

  private final BiFunction<InternalCache, RegionConfiguration, RegionAttributes<?, ?>> getRegionAttributes;
  private final TriConsumer<InternalCache, RegionConfiguration, Region<?, ?>> validateRegion;

  public CreateRegionFunction() {
    this(CacheFactoryStatics.getAnyInstance());
  }

  private CreateRegionFunction(InternalCache cache) {
    this(cache, createRegionConfigurationMetadataRegion(cache),
        (cache1, config) -> RegionHelper.getRegionAttributes(cache1, config),
        (cache1, config, region) -> RegionHelper.validateRegion(cache1, config, region));
  }

  @VisibleForTesting
  CreateRegionFunction(InternalCache cache,
      Region<String, RegionConfiguration> regionConfigurationsRegion,
      BiFunction<InternalCache, RegionConfiguration, RegionAttributes<?, ?>> getRegionAttributes,
      TriConsumer<InternalCache, RegionConfiguration, Region<?, ?>> validateRegion) {
    this.cache = cache;
    this.regionConfigurationsRegion = regionConfigurationsRegion;
    this.getRegionAttributes = getRegionAttributes;
    this.validateRegion = validateRegion;
  }

  @Override
  public void execute(FunctionContext context) {
    RegionConfiguration configuration = (RegionConfiguration) context.getArguments();
    if (logger.isDebugEnabled()) {
      logger.debug("Function {} received request: {}", ID, configuration);
    }

    // Create or retrieve region
    RegionStatus status = createOrRetrieveRegion(configuration);

    // Dump XML
    if (DUMP_SESSION_CACHE_XML) {
      writeCacheXml();
    }

    // Return status
    context.getResultSender().lastResult(status);
  }

  @Override
  public Collection<ResourcePermission> getRequiredPermissions(String regionName) {
    return singletonList(ResourcePermissions.DATA_MANAGE);
  }

  @Override
  public String getId() {
    return ID;
  }

  @Override
  public void toData(DataOutput out) {
    // nothing
  }

  @Override
  public void fromData(DataInput in) {
    // nothing
  }

  private RegionStatus createOrRetrieveRegion(RegionConfiguration configuration) {
    String regionName = configuration.getRegionName();

    if (logger.isDebugEnabled()) {
      logger.debug("Function {} retrieving region named: {}", ID, regionName);
    }

    Region region = cache.getRegion(regionName);
    RegionStatus status;
    if (region == null) {
      status = createRegion(configuration);
    } else {
      status = RegionStatus.VALID;
      try {
        RegionAttributes existingRegionAttributes = region.getAttributes();
        RegionAttributes requestedRegionAttributes =
            getRegionAttributes.apply(cache, configuration);
        compareRegionAttributes(existingRegionAttributes, requestedRegionAttributes);
      } catch (Exception e) {
        if (!e.getMessage().equals("CacheListeners are not the same")) {
          logger.warn(e);
        }

        status = RegionStatus.INVALID;
      }
    }

    return status;
  }

  private RegionStatus createRegion(RegionConfiguration configuration) {
    // Get a distributed lock
    DistributedMemberLock dml = getDistributedLock();
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Attempting to lock {}", this, dml);
    }
    RegionStatus status;
    try {
      long start = 0;
      if (logger.isDebugEnabled()) {
        start = System.currentTimeMillis();
      }
      // Obtain a lock on the distributed lock
      dml.lockInterruptibly();
      if (logger.isDebugEnabled()) {
        long end = System.currentTimeMillis();
        logger.debug("{}: Obtained lock on {} in {} ms", this, dml, end - start);
      }

      // Attempt to get the region again after the lock has been obtained
      String regionName = configuration.getRegionName();
      Region region = cache.getRegion(regionName);

      // If it exists now, validate it.
      // Else put an entry into the sessionRegionConfigurationsRegion
      // while holding the lock. This will create the region in all VMs.
      if (region == null) {
        regionConfigurationsRegion.put(regionName, configuration);

        // Retrieve the region after creating it
        region = cache.getRegion(regionName);
        // If the region is null now, it wasn't created for some reason
        // (e.g. the region attributes id were invalid)
        if (region == null) {
          status = RegionStatus.INVALID;
        } else {
          // Create the PR buckets if necessary)
          if (region instanceof PartitionedRegion) {
            PartitionedRegion pr = (PartitionedRegion) region;
            createBuckets(pr);
          }
          status = RegionStatus.VALID;
        }
      } else {
        status = RegionStatus.VALID;
        try {
          RegionHelper.validateRegion(cache, configuration, region); // TODO:KIRK
        } catch (Exception e) {
          if (!e.getMessage().equals("CacheListeners are not the same")) {
            logger.warn(e);
          }
          status = RegionStatus.INVALID;
        }
      }
    } catch (Exception e) {
      logger.warn("{}: Caught Exception attempting to create region named {}:", this,
          configuration.getRegionName(), e);
      status = RegionStatus.INVALID;
    } finally {
      // Unlock the distributed lock
      try {
        dml.unlock();
      } catch (Exception ignore) {
      }
    }
    return status;
  }

  private void createBuckets(PartitionedRegion region) {
    PartitionRegionHelper.assignBucketsToPartitions(region); // TODO:KIRK
  }

  private void writeCacheXml() {
    File file = new File("cache-" + System.currentTimeMillis() + ".xml");

    try {
      PrintWriter pw = new PrintWriter(new FileWriter(file), true);
      CacheXmlGenerator.generate(cache, pw);
      pw.close();
    } catch (IOException ignored) {
    }
  }

  private DistributedMemberLock getDistributedLock() {
    String dlsName = regionConfigurationsRegion.getName();
    DistributedLockService lockService = initializeDistributedLockService(dlsName);
    String lockToken = dlsName + "_token";

    return new DistributedMemberLock(lockService, lockToken);
  }

  private DistributedLockService initializeDistributedLockService(String dlsName) {
    DistributedLockService lockService = DistributedLockService.getServiceNamed(dlsName);
    if (lockService == null) {
      lockService = DistributedLockService.create(dlsName, cache.getDistributedSystem()); // TODO:KIRK
    }

    return lockService;
  }

  /**
   * If the existing region was using the DEFAULT diskstore but it was not explicitly linked to the
   * region using setDiskStore or disk-store-name tags. diskStoreName is set as null. This is
   * interpreted by Geode as use the DEFAULT diskstore.
   * This link between the existing region and a diskstore may happen because the diskstore is
   * named as DEFAULT.
   * The user may change the default location of the DEFAULT diskstore but the AppServer always
   * requests it be at the default location.
   * This comparison with always used to fail and the AppServer could not start up.
   * The goal of this method is that if both existing region and requested region are using the
   * DEFAULT diskstore, the existing regions take precedence and the requested ones are ignored.
   * This is current behavior which can be seen in
   * {@link RegionAttributesCreation#sameAs(RegionAttributes)}
   * The logic is to intercept the configurations for the regions and only if the both the regions
   * have diskStoreName set to null, meaning both should use the DEFAULT diskstore, the diskstore
   * names are sent as DEFAULT in the configuration and send to geode-core for comparison for rest
   * of the region attributes.
   */
  @VisibleForTesting
  static void compareRegionAttributes(RegionAttributes existingRegionAttributes,
      RegionAttributes requestedRegionAttributes) {
    EvictionAttributes evictionAttributes = existingRegionAttributes.getEvictionAttributes();
    RegionAttributesCreation existingRACreation =
        new RegionAttributesCreation(existingRegionAttributes, false);
    RegionAttributesCreation requestedAttributesCreation =
        new RegionAttributesCreation(requestedRegionAttributes, false);

    if (existingRegionAttributes.getDataPolicy().withPersistence() ||
        evictionAttributes != null &&
            evictionAttributes.getAction() == EvictionAction.OVERFLOW_TO_DISK) {
      if (requestedRegionAttributes.getDiskStoreName() == null &&
          existingRegionAttributes.getDiskStoreName() == null) {
        existingRACreation.setDiskStoreName("DEFAULT");
        requestedAttributesCreation.setDiskStoreName("DEFAULT");
      }
    }

    existingRACreation.sameAs(requestedAttributesCreation);
  }

  private static Region<String, RegionConfiguration> createRegionConfigurationMetadataRegion(
      InternalCache cache) {
    // a sessionFactory in hibernate could have been re-started
    // so, it is possible that this region exists already
    Region<String, RegionConfiguration> region =
        cache.getRegion(REGION_CONFIGURATION_METADATA_REGION);

    if (region != null) {
      return region;
    }

    InternalRegionFactory<String, RegionConfiguration> regionFactory =
        cache.createInternalRegionFactory(RegionShortcut.REPLICATE);
    regionFactory.addCacheListener(new RegionConfigurationCacheListener());
    regionFactory.setInternalRegion(true);
    return regionFactory.create(REGION_CONFIGURATION_METADATA_REGION);
  }

  private static final long serialVersionUID = -9210226844302128969L;
}
