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
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * Test that the bucket size does not go negative when
 * we fault out and in a delta object.
 * @author dsmith
 *
 */
public class Bug42055DUnitTest extends CacheTestCase {
  

  /**
   * @param name
   */
  public Bug42055DUnitTest(String name) {
    super(name);
  }

  public void testPROverflow() throws Exception {
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    SerializableCallable createDataRegion = new SerializableCallable("createDataRegion") {
      public Object call() throws Exception
      {
        Cache cache = getCache();
        AttributesFactory attr = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        PartitionAttributes prAttr = paf.create();
        attr.setPartitionAttributes(prAttr);
        attr.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(1, EvictionAction.OVERFLOW_TO_DISK));
        Region region = cache.createRegion("region1", attr.create());
        
        return null;
      }
    };
    
    vm0.invoke(createDataRegion);
    
    SerializableRunnable createEmptyRegion = new SerializableRunnable("createEmptyRegion") {
      public void run()
      {
        Cache cache = getCache();
        AttributesFactory<Integer, TestDelta> attr = new AttributesFactory<Integer, TestDelta>();
        PartitionAttributesFactory<Integer, TestDelta> paf = new PartitionAttributesFactory<Integer, TestDelta>();
        paf.setLocalMaxMemory(0);
        PartitionAttributes<Integer, TestDelta> prAttr = paf.create();
        attr.setPartitionAttributes(prAttr);
        attr.setDataPolicy(DataPolicy.PARTITION);
        attr.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(1, EvictionAction.OVERFLOW_TO_DISK));
        Region<Integer, TestDelta> region = cache.createRegion("region1", attr.create());
      }
    };
    
    vm1.invoke(createEmptyRegion);
  }
}
