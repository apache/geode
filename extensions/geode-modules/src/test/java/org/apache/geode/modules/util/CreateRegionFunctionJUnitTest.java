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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.CustomExpiry;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskWriteAttributes;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.MembershipAttributes;
import org.apache.geode.cache.MirrorType;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.SubscriptionAttributes;
import org.apache.geode.compression.Compressor;
import org.apache.geode.internal.cache.EvictionAttributesImpl;

public class CreateRegionFunctionJUnitTest {

  RegionAttributes getRegionAttributesWithModifiedDiskDirSize(final int diskDirSize) {
    return new RegionAttributes() {
      @Override
      public CacheLoader getCacheLoader() {
        return null;
      }

      @Override
      public CacheWriter getCacheWriter() {
        return null;
      }

      @Override
      public Class getKeyConstraint() {
        return null;
      }

      @Override
      public Class getValueConstraint() {
        return null;
      }

      @Override
      public ExpirationAttributes getRegionTimeToLive() {
        return new ExpirationAttributes();
      }

      @Override
      public ExpirationAttributes getRegionIdleTimeout() {
        return new ExpirationAttributes();
      }

      @Override
      public ExpirationAttributes getEntryTimeToLive() {
        return new ExpirationAttributes();
      }

      @Override
      public ExpirationAttributes getEntryIdleTimeout() {
        return new ExpirationAttributes();
      }

      @Override
      public CustomExpiry getCustomEntryTimeToLive() {
        return null;
      }

      @Override
      public CustomExpiry getCustomEntryIdleTimeout() {
        return null;
      }

      @Override
      public boolean getIgnoreJTA() {
        return false;
      }

      @Override
      public MirrorType getMirrorType() {
        return null;
      }

      @Override
      public DataPolicy getDataPolicy() {
        return DataPolicy.NORMAL;
      }

      @Override
      public Scope getScope() {
        return null;
      }

      @Override
      public EvictionAttributes getEvictionAttributes() {
        return new EvictionAttributesImpl().setAction(EvictionAction.OVERFLOW_TO_DISK);
      }

      @Override
      public CacheListener getCacheListener() {
        return null;
      }

      @Override
      public CacheListener[] getCacheListeners() {
        return new CacheListener[0];
      }

      @Override
      public int getInitialCapacity() {
        return 0;
      }

      @Override
      public float getLoadFactor() {
        return 0;
      }

      @Override
      public boolean isLockGrantor() {
        return false;
      }

      @Override
      public boolean getMulticastEnabled() {
        return false;
      }

      @Override
      public int getConcurrencyLevel() {
        return 0;
      }

      @Override
      public boolean getPersistBackup() {
        return false;
      }

      @Override
      public DiskWriteAttributes getDiskWriteAttributes() {
        return null;
      }

      @Override
      public File[] getDiskDirs() {
        return new File[0];
      }

      @Override
      public boolean getIndexMaintenanceSynchronous() {
        return false;
      }

      @Override
      public PartitionAttributes getPartitionAttributes() {
        return null;
      }

      @Override
      public MembershipAttributes getMembershipAttributes() {
        return new MembershipAttributes();
      }

      @Override
      public SubscriptionAttributes getSubscriptionAttributes() {
        return null;
      }

      @Override
      public boolean getStatisticsEnabled() {
        return false;
      }

      @Override
      public boolean getEarlyAck() {
        return false;
      }

      @Override
      public boolean getPublisher() {
        return false;
      }

      @Override
      public boolean getEnableConflation() {
        return false;
      }

      @Override
      public boolean getEnableBridgeConflation() {
        return false;
      }

      @Override
      public boolean getEnableSubscriptionConflation() {
        return false;
      }

      @Override
      public boolean getEnableAsyncConflation() {
        return false;
      }

      @Override
      public int[] getDiskDirSizes() {
        return new int[] {diskDirSize};
      }

      @Override
      public String getPoolName() {
        return null;
      }

      @Override
      public boolean getCloningEnabled() {
        return false;
      }

      @Override
      public String getDiskStoreName() {
        return null;
      }

      @Override
      public boolean isDiskSynchronous() {
        return false;
      }

      @Override
      public Set<String> getGatewaySenderIds() {
        return new HashSet<String>();
      }

      @Override
      public Set<String> getAsyncEventQueueIds() {
        return new HashSet<String>();
      }

      @Override
      public boolean getConcurrencyChecksEnabled() {
        return false;
      }

      @Override
      public Compressor getCompressor() {
        return null;
      }

      @Override
      public boolean getOffHeap() {
        return false;
      }
    };
  }

  @Test
  public void whenDiskStoreNamesForBothAreNullAndDiskPropertiesAreDifferentThenRegionComparisonMustBeSuccessful() {
    CreateRegionFunction createRegionFunction = mock(CreateRegionFunction.class);
    doCallRealMethod().when(createRegionFunction).compareRegionAttributes(any(), any());
    RegionAttributes existingRegionAttributes = getRegionAttributesWithModifiedDiskDirSize(1);
    RegionAttributes requestedRegionAttributes = getRegionAttributesWithModifiedDiskDirSize(2);
    createRegionFunction.compareRegionAttributes(existingRegionAttributes,
        requestedRegionAttributes);
  }

}
