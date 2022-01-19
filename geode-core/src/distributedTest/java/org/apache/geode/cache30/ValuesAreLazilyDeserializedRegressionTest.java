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
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;

/**
 * Test to make sure cache values are lazily deserialized
 *
 * <p>
 * TRAC #34948: distributed cache values are always getting deserialized
 *
 * @since GemFire 5.0
 */

public class ValuesAreLazilyDeserializedRegressionTest extends CacheTestCase {

  private static final String REGION_NAME = "bug34948";

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
  public void valueShouldBeLazilyDeserialized() throws Exception {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.PRELOADED);

    Region<String, HomeBoy> region = createRootRegion(REGION_NAME, factory.create());

    // before gii
    region.put("key1", new HomeBoy());

    doCreateOtherVm(otherVM);

    // after gii
    region.put("key2", new HomeBoy());

    region.localDestroy("key1");
    region.localDestroy("key2");

    Object value = region.get("key1");
    assertTrue(region.get("key1") instanceof HomeBoy);
    assertTrue(region.get("key2") == null); // preload will not distribute

    // TODO: add putAll test once it does not deserialize
  }

  private void doCreateOtherVm(final VM otherVM) {
    otherVM.invoke(new CacheSerializableRunnable("create root") {

      @Override
      public void run2() throws CacheException {
        getSystem();

        CacheListener<String, HomeBoy> listener = new CacheListenerAdapter<String, HomeBoy>() {
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

        AttributesFactory<String, HomeBoy> factory = new AttributesFactory<>();
        factory.setScope(Scope.DISTRIBUTED_ACK);
        factory.setDataPolicy(DataPolicy.PRELOADED);
        factory.setCacheListener(listener);

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
