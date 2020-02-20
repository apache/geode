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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Collections;

import org.apache.geode.DataSerializable;
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
import org.apache.geode.management.internal.security.ResourcePermissions;
import org.apache.geode.security.ResourcePermission;

public class CreateRegionFunction implements Function, Declarable, DataSerializable {
  private static final long serialVersionUID = -9210226844302128969L;
  public static final String ID = "create-region-function";
  private static final boolean DUMP_SESSION_CACHE_XML =
      Boolean.getBoolean("gemfiremodules.dumpSessionCacheXml");
  static final String REGION_CONFIGURATION_METADATA_REGION =
      "__regionConfigurationMetadata";

  private final InternalCache cache;
  private final Region<String, RegionConfiguration> regionConfigurationsRegion;

  public CreateRegionFunction() {
    this.cache = CacheFactoryStatics.getAnyInstance();
    this.regionConfigurationsRegion = createRegionConfigurationMetadataRegion();
  }

  @Override
  @SuppressWarnings("unchecked")
  public void execute(FunctionContext context) {
    RegionConfiguration configuration = (RegionConfiguration) context.getArguments();
    if (this.cache.getLogger().fineEnabled()) {
      this.cache.getLogger().fine("Function " + ID + " received request: " + configuration);
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
    return Collections.singletonList(ResourcePermissions.DATA_MANAGE);
  }

  private RegionStatus createOrRetrieveRegion(RegionConfiguration configuration) {
    RegionStatus status;
    String regionName = configuration.getRegionName();

    if (this.cache.getLogger().fineEnabled()) {
      this.cache.getLogger().fine("Function " + ID + " retrieving region named: " + regionName);
    }

    Region region = this.cache.getRegion(regionName);
    if (region == null) {
      status = createRegion(configuration);
    } else {
      status = RegionStatus.VALID;
      try {
        RegionAttributes existingRegionAttributes = region.getAttributes();
        RegionAttributes requestedRegionAttributes =
            RegionHelper.getRegionAttributes(this.cache, configuration);
        compareRegionAttributes(existingRegionAttributes, requestedRegionAttributes);
      } catch (Exception e) {
        if (!e.getMessage()
            .equals("CacheListeners are not the same")) {
          this.cache.getLogger().warning(e);
        }

        status = RegionStatus.INVALID;
      }
    }

    return status;
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
  void compareRegionAttributes(RegionAttributes existingRegionAttributes,
      RegionAttributes requestedRegionAttributes) {
    EvictionAttributes evictionAttributes = existingRegionAttributes.getEvictionAttributes();
    RegionAttributesCreation existingRACreation =
        new RegionAttributesCreation(existingRegionAttributes, false);
    RegionAttributesCreation requestedAttributesCreation =
        new RegionAttributesCreation(requestedRegionAttributes, false);

    if (existingRegionAttributes.getDataPolicy().withPersistence() || (evictionAttributes != null
        && evictionAttributes.getAction() == EvictionAction.OVERFLOW_TO_DISK)) {
      if (requestedRegionAttributes.getDiskStoreName() == null
          && existingRegionAttributes.getDiskStoreName() == null) {
        existingRACreation.setDiskStoreName("DEFAULT");
        requestedAttributesCreation.setDiskStoreName("DEFAULT");
      }
    }

    existingRACreation.sameAs(requestedAttributesCreation);
  }

  @Override
  public String getId() {
    return ID;
  }

  @Override
  public boolean optimizeForWrite() {
    return false;
  }

  @Override
  public boolean isHA() {
    return true;
  }

  @Override
  public boolean hasResult() {
    return true;
  }

  private RegionStatus createRegion(RegionConfiguration configuration) {
    // Get a distributed lock
    DistributedMemberLock dml = getDistributedLock();
    if (this.cache.getLogger().fineEnabled()) {
      this.cache.getLogger().fine(this + ": Attempting to lock " + dml);
    }
    long start = 0, end;
    RegionStatus status;
    try {
      if (this.cache.getLogger().fineEnabled()) {
        start = System.currentTimeMillis();
      }
      // Obtain a lock on the distributed lock
      dml.lockInterruptibly();
      if (this.cache.getLogger().fineEnabled()) {
        end = System.currentTimeMillis();
        this.cache.getLogger()
            .fine(this + ": Obtained lock on " + dml + " in " + (end - start) + " ms");
      }

      // Attempt to get the region again after the lock has been obtained
      String regionName = configuration.getRegionName();
      Region region = this.cache.getRegion(regionName);

      // If it exists now, validate it.
      // Else put an entry into the sessionRegionConfigurationsRegion
      // while holding the lock. This will create the region in all VMs.
      if (region == null) {
        this.regionConfigurationsRegion.put(regionName, configuration);

        // Retrieve the region after creating it
        region = this.cache.getRegion(regionName);
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
          RegionHelper.validateRegion(this.cache, configuration, region);
        } catch (Exception e) {
          if (!e.getMessage()
              .equals("CacheListeners are not the same")) {
            this.cache.getLogger().warning(e);
          }
          status = RegionStatus.INVALID;
        }
      }
    } catch (Exception e) {
      String builder = this + ": Caught Exception attempting to create region named "
          + configuration.getRegionName() + ":";
      this.cache.getLogger().warning(builder, e);
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
    PartitionRegionHelper.assignBucketsToPartitions(region);
  }

  private Region<String, RegionConfiguration> createRegionConfigurationMetadataRegion() {
    // a sessionFactory in hibernate could have been re-started
    // so, it is possible that this region exists already
    Region<String, RegionConfiguration> region =
        this.cache.getRegion(REGION_CONFIGURATION_METADATA_REGION);

    if (region != null) {
      return region;
    }

    InternalRegionFactory<String, RegionConfiguration> regionFactory =
        cache.createInternalRegionFactory(RegionShortcut.REPLICATE);
    regionFactory.addCacheListener(new RegionConfigurationCacheListener());
    regionFactory.setInternalRegion(true);
    return regionFactory.create(REGION_CONFIGURATION_METADATA_REGION);
  }

  private void writeCacheXml() {
    File file = new File("cache-" + System.currentTimeMillis() + ".xml");

    try {
      PrintWriter pw = new PrintWriter(new FileWriter(file), true);
      CacheXmlGenerator.generate(this.cache, pw);
      pw.close();
    } catch (IOException ignored) {
    }
  }

  private DistributedMemberLock getDistributedLock() {
    String dlsName = this.regionConfigurationsRegion.getName();
    DistributedLockService lockService = initializeDistributedLockService(dlsName);
    String lockToken = dlsName + "_token";

    return new DistributedMemberLock(lockService, lockToken);
  }

  private DistributedLockService initializeDistributedLockService(String dlsName) {
    DistributedLockService lockService = DistributedLockService.getServiceNamed(dlsName);
    if (lockService == null) {
      lockService = DistributedLockService.create(dlsName, this.cache.getDistributedSystem());
    }

    return lockService;
  }

  @Override
  public void toData(DataOutput out) {}

  @Override
  public void fromData(DataInput in) {}
}
