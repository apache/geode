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
 * @since GemFire 5.0
 */
@Category(DistributedTest.class)
public class Bug34948DUnitTest extends JUnit4CacheTestCase {

  public Bug34948DUnitTest() {
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
        af.setScope(Scope.DISTRIBUTED_ACK);
        af.setDataPolicy(DataPolicy.PRELOADED);
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
        createRootRegion("bug34948", af.create());
      }
    });
  }

  /**
   * Make sure that value is only deserialized in cache whose application asks for the value.
   */
  @Test
  public void testBug34948() throws CacheException {
    final AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.PRELOADED);
    final Region r = createRootRegion("bug34948", factory.create());

    // before gii
    r.put("key1", new HomeBoy());

    doCreateOtherVm();

    // after gii
    r.put("key2", new HomeBoy());

    r.localDestroy("key1");
    r.localDestroy("key2");

    Object o = r.get("key1");
    assertTrue(r.get("key1") instanceof HomeBoy);
    assertTrue(r.get("key2") == null); // preload will not distribute

    // @todo darrel: add putAll test once it does not deserialize
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
