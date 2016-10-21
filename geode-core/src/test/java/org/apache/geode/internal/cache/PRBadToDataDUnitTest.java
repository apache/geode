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
package org.apache.geode.internal.cache;

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

import java.io.*;

import org.apache.geode.*;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.cache30.CacheTestCase;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;

/**
 * Tests a toData that always throws an IOException. This test does a put with the bad to data from
 * an accessor to see if it will keep trying to resend the put to the data store
 * 
 * 
 */
@Category(DistributedTest.class)
public class PRBadToDataDUnitTest extends JUnit4CacheTestCase {

  @Test
  public void testBadToData() {
    final Host host = Host.getHost(0);
    final VM vm1 = host.getVM(0);
    final VM vm2 = host.getVM(1);
    final String name = "PR_TEMP";

    final SerializableRunnable create = new CacheSerializableRunnable("Create PR accessor ") {
      public void run2() {
        final AttributesFactory factory = new AttributesFactory();
        factory.setPartitionAttributes(
            new PartitionAttributesFactory().setRedundantCopies(0).setLocalMaxMemory(0).create());
        final PartitionedRegion pr = (PartitionedRegion) createRootRegion(name, factory.create());
        assertNotNull(pr);
      }
    };
    vm1.invoke(create);

    final SerializableRunnable create2 = new SerializableRunnable("Create PR dataStore ") {
      public void run() {
        try {
          final AttributesFactory factory = new AttributesFactory();
          factory.setPartitionAttributes(new PartitionAttributesFactory().setRedundantCopies(0)
              .setLocalMaxMemory(100).create());
          final PartitionedRegion pr = (PartitionedRegion) createRootRegion(name, factory.create());
          assertNotNull(pr);
        } catch (final CacheException ex) {
          Assert.fail("While creating Partitioned region", ex);
        }
      }
    };
    vm2.invoke(create2);

    final SerializableRunnable putData = new SerializableRunnable("Puts Data") {
      public void run() {
        final PartitionedRegion pr = (PartitionedRegion) getRootRegion(name);
        assertNotNull(pr);
        try {
          pr.put("key", new DataSerializable() {
            public void toData(DataOutput out) throws IOException {
              throw new IOException("bad to data");
              // throw new ToDataException("bad to data");
            }

            public void fromData(DataInput in) throws IOException, ClassNotFoundException {
              // nothing needed
            }
          });
          fail("expected ToDataException");
        } catch (ToDataException expected) {
          // we want this put to fail with an exception instead of hanging due to retries
        }
      }
    };
    vm1.invoke(putData);
  }
}
