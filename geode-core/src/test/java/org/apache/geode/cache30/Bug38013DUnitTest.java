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
package org.apache.geode.cache30;

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;

/**
 * Test to make sure PR cache values are lazily deserialized
 *
 * @since GemFire 5.0
 */
@Category(DistributedTest.class)
public class Bug38013DUnitTest extends JUnit4CacheTestCase {

  public Bug38013DUnitTest() {
    super();
  }

  ////////////////////// Test Methods //////////////////////

  private VM getOtherVm() {
    Host host = Host.getHost(0);
    return host.getVM(0);
  }

  static protected Object lastCallback = null;

  private void doCreateOtherVm() {
    VM vm = getOtherVm();
    vm.invoke(new CacheSerializableRunnable("create root") {
      public void run2() throws CacheException {
        getSystem();
        AttributesFactory af = new AttributesFactory();
        CacheListener cl = new CacheListenerAdapter() {
          public void afterCreate(EntryEvent event) {
            // getLogWriter().info("afterCreate " + event.getKey());
            if (event.getCallbackArgument() != null) {
              lastCallback = event.getCallbackArgument();
            }
          }

          public void afterUpdate(EntryEvent event) {
            // getLogWriter().info("afterUpdate " + event.getKey());
            if (event.getCallbackArgument() != null) {
              lastCallback = event.getCallbackArgument();
            }
          }

          public void afterInvalidate(EntryEvent event) {
            if (event.getCallbackArgument() != null) {
              lastCallback = event.getCallbackArgument();
            }
          }

          public void afterDestroy(EntryEvent event) {
            if (event.getCallbackArgument() != null) {
              lastCallback = event.getCallbackArgument();
            }
          }
        };
        af.setCacheListener(cl);
        // create a pr with a data store
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(0);
        // use defaults so this is a data store
        af.setPartitionAttributes(paf.create());
        createRootRegion("bug38013", af.create());
      }
    });
  }

  /**
   * Make sure that value is only deserialized in cache whose application asks for the value.
   */
  @Test
  public void testBug38013() throws CacheException {
    final AttributesFactory factory = new AttributesFactory();
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(0);
    paf.setLocalMaxMemory(0); // make it an accessor
    factory.setPartitionAttributes(paf.create());
    final Region r = createRootRegion("bug38013", factory.create());

    doCreateOtherVm();

    r.put("key1", new HomeBoy());

    assertTrue(r.get("key1") instanceof HomeBoy);
  }

  public static class HomeBoy implements DataSerializable {
    public HomeBoy() {}

    public void toData(DataOutput out) throws IOException {
      DistributedMember me = InternalDistributedSystem.getAnyInstance().getDistributedMember();
      DataSerializer.writeObject(me, out);
    }

    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      DistributedSystem ds = InternalDistributedSystem.getAnyInstance();
      DistributedMember me = ds.getDistributedMember();
      DistributedMember hb = (DistributedMember) DataSerializer.readObject(in);
      if (me.equals(hb)) {
        ds.getLogWriter().info("HomeBoy was deserialized on his home");
      } else {
        String msg = "HomeBoy was deserialized on " + me + " instead of his home " + hb;
        ds.getLogWriter().error(msg);
        throw new IllegalStateException(msg);
      }
    }
  }
}
