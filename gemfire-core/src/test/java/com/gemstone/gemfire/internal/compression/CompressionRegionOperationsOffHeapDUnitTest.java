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
package com.gemstone.gemfire.internal.compression;

import java.util.Properties;

import com.gemstone.gemfire.compression.Compressor;
import com.gemstone.gemfire.compression.SnappyCompressor;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.cache.OffHeapTestUtil;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;

public class CompressionRegionOperationsOffHeapDUnitTest extends
    CompressionRegionOperationsDUnitTest {

  public CompressionRegionOperationsOffHeapDUnitTest(String name) {
    super(name);
  }
  @Override
  public void tearDown2() throws Exception {
    SerializableRunnable checkOrphans = new SerializableRunnable() {

      @Override
      public void run() {
        if(hasCache()) {
          OffHeapTestUtil.checkOrphans();
        }
      }
    };
    invokeInEveryVM(checkOrphans);
    try {
      checkOrphans.run();
    } finally {
      super.tearDown2();
    }
  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties props = super.getDistributedSystemProperties();
    props.setProperty(DistributionConfig.OFF_HEAP_MEMORY_SIZE_NAME, "1m");
    return props;
  }

  @Override
  protected void createRegion() {
    Compressor compressor = null;
    try {
      compressor = SnappyCompressor.getDefaultInstance();;
    } catch (Throwable t) {
      // Not a supported OS
      return;
    }
    createCompressedRegionOnVm(getVM(TEST_VM), REGION_NAME, compressor, true);
  }

}
