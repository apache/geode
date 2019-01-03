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
import java.util.Properties;

import org.apache.geode.DataSerializable;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.internal.locks.DistributedMemberLock;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalRegionArguments;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.xmlcache.CacheXmlGenerator;
import org.apache.geode.management.internal.security.ResourcePermissions;
import org.apache.geode.security.ResourcePermission;

public class CreateRegionFunction implements Function, Declarable, DataSerializable {

  private static final long serialVersionUID = -9210226844302128969L;

  private final Cache cache;

  private final Region<String, RegionConfiguration> regionConfigurationsRegion;

  public static final String ID = "create-region-function";

  private static final boolean DUMP_SESSION_CACHE_XML =
      Boolean.getBoolean("gemfiremodules.dumpSessionCacheXml");

  static final String REGION_CONFIGURATION_METADATA_REGION =
      "__regionConfigurationMetadata";

  public CreateRegionFunction() {
    this.cache = CacheFactory.getAnyInstance();
    this.regionConfigurationsRegion = createRegionConfigurationMetadataRegion();
  }

  public void execute(FunctionContext context) {
    RegionConfiguration configuration = (RegionConfiguration) context.getArguments();
    if (this.cache.getLogger().fineEnabled()) {
      StringBuilder builder = new StringBuilder();
      builder.append("Function ").append(ID).append(" received request: ").append(configuration);
      this.cache.getLogger().fine(builder.toString());
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
    RegionStatus status = null;
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
        RegionHelper.validateRegion(this.cache, configuration, region);
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

  public String getId() {
    return ID;
  }

  public boolean optimizeForWrite() {
    return false;
  }

  public boolean isHA() {
    return true;
  }

  public boolean hasResult() {
    return true;
  }

  public void init(Properties properties) {}

  private RegionStatus createRegion(RegionConfiguration configuration) {
    // Get a distributed lock
    DistributedMemberLock dml = getDistributedLock();
    if (this.cache.getLogger().fineEnabled()) {
      this.cache.getLogger().fine(this + ": Attempting to lock " + dml);
    }
    long start = 0, end = 0;
    RegionStatus status = null;
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
      StringBuilder builder = new StringBuilder();
      builder.append(this).append(": Caught Exception attempting to create region named ")
          .append(configuration.getRegionName()).append(":");
      this.cache.getLogger().warning(builder.toString(), e);
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
    Region<String, RegionConfiguration> r =
        this.cache.getRegion(REGION_CONFIGURATION_METADATA_REGION);
    if (r != null) {
      return r;
    }
    GemFireCacheImpl gemFireCache = (GemFireCacheImpl) cache;
    InternalRegionArguments ira = new InternalRegionArguments().setInternalRegion(true);
    AttributesFactory af = new AttributesFactory();
    af.setDataPolicy(DataPolicy.REPLICATE);
    af.setScope(Scope.DISTRIBUTED_ACK);
    af.addCacheListener(new RegionConfigurationCacheListener());
    RegionAttributes ra = af.create();
    try {
      return gemFireCache.createVMRegion(REGION_CONFIGURATION_METADATA_REGION, ra, ira);
    } catch (IOException | ClassNotFoundException e) {
      InternalGemFireError assErr = new InternalGemFireError(
          "unexpected exception");
      assErr.initCause(e);
      throw assErr;
    }
  }

  private void writeCacheXml() {
    File file = new File("cache-" + System.currentTimeMillis() + ".xml");
    try {
      PrintWriter pw = new PrintWriter(new FileWriter(file), true);
      CacheXmlGenerator.generate(this.cache, pw);
      pw.close();
    } catch (IOException e) {
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
  public void toData(DataOutput out) throws IOException {

  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {

  }
}
