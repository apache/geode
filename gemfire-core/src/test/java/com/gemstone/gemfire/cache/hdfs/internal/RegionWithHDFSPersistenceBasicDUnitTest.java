/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.cache.hdfs.internal;

import java.io.File;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.hdfs.HDFSStoreFactory;
import com.gemstone.gemfire.internal.cache.LocalRegion;

import dunit.SerializableCallable;

@SuppressWarnings({ "serial", "rawtypes", "deprecation" })
public class RegionWithHDFSPersistenceBasicDUnitTest extends
    RegionWithHDFSBasicDUnitTest {

  public RegionWithHDFSPersistenceBasicDUnitTest(String name) {
    super(name);
  }

  @Override
  protected SerializableCallable getCreateRegionCallable(final int totalnumOfBuckets,
      final int batchSizeMB, final int maximumEntries, final String folderPath,
      final String uniqueName, final int batchInterval, final boolean queuePersistent,
      final boolean writeonly, final long timeForRollover, final long maxFileSize) {
    SerializableCallable createRegion = new SerializableCallable() {
      public Object call() throws Exception {
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.HDFS_PERSISTENT_PARTITION);
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setTotalNumBuckets(totalnumOfBuckets);
        paf.setRedundantCopies(1);
        
        af.setHDFSStoreName(uniqueName);
        
        af.setPartitionAttributes(paf.create());
        HDFSStoreFactory hsf = getCache().createHDFSStoreFactory();
        // Going two level up to avoid home directories getting created in
        // VM-specific directory. This avoids failures in those tests where
        // datastores are restarted and bucket ownership changes between VMs.
        homeDir = new File(tmpDir + "/../../" + folderPath).getCanonicalPath();
        hsf.setHomeDir(homeDir);
        hsf.setBatchSize(batchSizeMB);
        hsf.setBufferPersistent(queuePersistent);
        hsf.setMaxMemory(3);
        hsf.setBatchInterval(batchInterval);
        if (timeForRollover != -1) {
          hsf.setWriteOnlyFileRolloverInterval((int)timeForRollover);
          System.setProperty("gemfire.HDFSRegionDirector.FILE_ROLLOVER_TASK_INTERVAL_SECONDS", "1");
        }
        if (maxFileSize != -1) {
          hsf.setWriteOnlyFileRolloverSize((int) maxFileSize);
        }
        hsf.create(uniqueName);
        
        af.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(maximumEntries, EvictionAction.LOCAL_DESTROY));
        
        af.setHDFSWriteOnly(writeonly);
        Region r = createRootRegion(uniqueName, af.create());
        ((LocalRegion)r).setIsTest();
        
        return 0;
      }
    };
    return createRegion;
  }
}
