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
package org.apache.geode.internal;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.DataSerializable;
import org.apache.geode.SerializationException;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.MembershipTest;

@Category({MembershipTest.class})
public class ClassNotFoundExceptionDUnitTest extends JUnit4CacheTestCase {

  public ClassNotFoundExceptionDUnitTest() {
    super();
  }

  @Test
  public void testDataSerializable() throws InterruptedException {
    doTest((ObjectFactory) () -> new ClassNotFoundDataSerializable());
  }

  @Test
  public void testPdx() throws InterruptedException {
    doTest((ObjectFactory) () -> new ClassNotFoundPdx(false));
  }

  public void doTest(final ObjectFactory objectFactory) throws InterruptedException {
    IgnoredException.addIgnoredException("SerializationException");
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);


    int port1 = createServerRegion(vm0);
    int port2 = createServerRegion(vm1);
    createClientRegion(vm2, port1);
    createClientRegion(vm3, port2);

    SerializableRunnable putKey = new SerializableRunnable() {
      @Override
      public void run() {
        Region region = getCache().getRegion("testSimplePdx");
        region.put("a", "b");
        region.put("b", "b");
        for (int i = 0; i < 10; i++) {
          region.put(i, i);
        }
        if (!region.containsKey("test")) {
          region.put("test", objectFactory.get());
        }
        try {
          region.put(objectFactory.get(), objectFactory.get());
          fail("Should have received an exception");
        } catch (SerializationException expected) {
          // ok
        } catch (ServerOperationException expected) {
          if (!(expected.getCause() instanceof SerializationException)
              && !(expected.getCause() instanceof ClassNotFoundException)) {
            throw expected;
          }
        }
        // try {
        // region.replace("test", objectFactory.get(), objectFactory.get());
        // fail("Should have received an exception");
        // } catch(SerializationException expected) {
        // //ok
        // } catch(ServerOperationException expected) {
        // if(!(expected.getCause() instanceof SerializationException) && !(expected.getCause()
        // instanceof ClassNotFoundException)) {
        // throw expected;
        // }
        // }
      }
    };

    SerializableRunnable getValue = new SerializableRunnable() {
      @Override
      public void run() {
        Region region = getCache().getRegion("testSimplePdx");
        try {
          assertNotNull(region.get("test"));
          fail("Should have received an exception");
        } catch (SerializationException expected) {
          // ok
        } catch (ServerOperationException expected) {
          if (!(expected.getCause() instanceof SerializationException)
              && !(expected.getCause() instanceof ClassNotFoundException)) {
            throw expected;
          }
        }
      }
    };

    SerializableRunnable registerInterest = new SerializableRunnable() {
      @Override
      public void run() {
        Region region = getCache().getRegion("testSimplePdx");

        try {
          ArrayList keys = new ArrayList();
          for (int i = 0; i < 1000; i++) {
            keys.add(i);
          }
          keys.add("test");
          region.getAll(keys);
          fail("Should have received an exception");
        } catch (SerializationException expected) {
          System.out.println("hi");
          // ok
        } catch (ServerOperationException expected) {
          if (!(expected.getCause() instanceof SerializationException)
              && !(expected.getCause() instanceof ClassNotFoundException)) {
            throw expected;
          }
        }
      }
    };


    vm2.invoke(putKey);


    vm1.invoke(getValue);

    vm3.invoke(getValue);
    vm3.invoke(registerInterest);
    vm1.invoke(putKey);
  }

  private int createServerRegion(VM vm) {
    SerializableCallable createRegion = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        AttributesFactory af = new AttributesFactory();
        // af.setScope(Scope.DISTRIBUTED_ACK);
        af.setDataPolicy(DataPolicy.PARTITION);
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        af.setPartitionAttributes(paf.create());
        createRootRegion("testSimplePdx", af.create());

        CacheServer server = getCache().addCacheServer();
        int port = AvailablePortHelper.getRandomAvailableTCPPort();
        server.setPort(port);
        server.start();
        return port;
      }
    };

    return (Integer) vm.invoke(createRegion);
  }

  private void createClientRegion(final VM vm, final int port) {
    SerializableCallable createRegion = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        disconnectFromDS();
        ClientCacheFactory cf = new ClientCacheFactory();
        cf.addPoolServer(NetworkUtils.getServerHostName(vm.getHost()), port);
        cf.setPoolSubscriptionEnabled(true);
        ClientCache cache = getClientCache(cf);
        cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create("testSimplePdx");
        return null;
      }
    };
    vm.invoke(createRegion);
  }

  private static class ClassNotFoundDataSerializable implements DataSerializable {

    public ClassNotFoundDataSerializable() {

    }

    @Override
    public void toData(DataOutput out) throws IOException {}

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      throw new ClassNotFoundException("Test exception");
    }
  }

  public static class ClassNotFoundPdx implements PdxSerializable {

    public ClassNotFoundPdx(boolean throwIt) {

    }

    public ClassNotFoundPdx() throws ClassNotFoundException {
      throw new ClassNotFoundException("Test Exception");
    }

    @Override
    public void toData(PdxWriter writer) {
      writer.writeString("field1", "string");

    }

    @Override
    public void fromData(PdxReader reader) {

    }
  }

  private interface ObjectFactory extends Serializable {
    Object get();
  }

}
