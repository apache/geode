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

import static org.junit.Assert.assertTrue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

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
 * <p>
 * TRAC #38013: PR regions do deserialization on remote bucket during get causing
 * NoClassDefFoundError
 *
 * <p>
 * Remote PartitionedRegion values should not be deserialized
 *
 * @since GemFire 5.0
 */

public class RemotePRValuesAreNotDeserializedRegressionTest extends CacheTestCase {

  private static final String REGION_NAME = "bug38013";

  // TODO: value of lastCallback is not validated
  private static Object lastCallback = null;

  private VM otherVM;

  @Before
  public void setUp() throws Exception {
    otherVM = Host.getHost(0).getVM(0);
  }

  /**
   * Make sure that value is only deserialized in cache whose application asks for the value.
   */
  @Test
  public void remotePRValuesShouldNotBeDeserialized() throws Exception {
    PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setRedundantCopies(0);
    partitionAttributesFactory.setLocalMaxMemory(0); // make it an accessor

    AttributesFactory factory = new AttributesFactory();
    factory.setPartitionAttributes(partitionAttributesFactory.create());

    Region<String, HomeBoy> region = createRootRegion(REGION_NAME, factory.create());

    doCreateOtherVm(otherVM);

    region.put("key1", new HomeBoy());

    assertTrue(region.get("key1") instanceof HomeBoy);
  }

  private void doCreateOtherVm(final VM otherVM) {
    otherVM.invoke(new CacheSerializableRunnable("create root") {

      @Override
      public void run2() throws CacheException {
        getSystem();

        CacheListener listener = new CacheListenerAdapter() {
          @Override
          public void afterCreate(final EntryEvent event) {
            if (event.getCallbackArgument() != null) {
              lastCallback = event.getCallbackArgument();
            }
          }

          @Override
          public void afterUpdate(final EntryEvent event) {
            if (event.getCallbackArgument() != null) {
              lastCallback = event.getCallbackArgument();
            }
          }

          @Override
          public void afterInvalidate(final EntryEvent event) {
            if (event.getCallbackArgument() != null) {
              lastCallback = event.getCallbackArgument();
            }
          }

          @Override
          public void afterDestroy(final EntryEvent event) {
            if (event.getCallbackArgument() != null) {
              lastCallback = event.getCallbackArgument();
            }
          }
        };

        AttributesFactory factory = new AttributesFactory();
        factory.setCacheListener(listener);

        // create a pr with a data store
        PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();
        partitionAttributesFactory.setRedundantCopies(0);

        // use defaults so this is a data store
        factory.setPartitionAttributes(partitionAttributesFactory.create());
        createRootRegion(REGION_NAME, factory.create());
      }
    });
  }

  private static class HomeBoy implements DataSerializable {

    public HomeBoy() {
      // nothing
    }

    @Override
    public void toData(final DataOutput out) throws IOException {
      DistributedMember me = InternalDistributedSystem.getAnyInstance().getDistributedMember();
      DataSerializer.writeObject(me, out);
    }

    @Override
    public void fromData(final DataInput in) throws IOException, ClassNotFoundException {
      DistributedSystem ds = InternalDistributedSystem.getAnyInstance();
      DistributedMember me = ds.getDistributedMember();
      DistributedMember hb = DataSerializer.readObject(in);
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
