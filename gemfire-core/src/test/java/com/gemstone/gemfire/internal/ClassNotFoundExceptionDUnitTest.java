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
package com.gemstone.gemfire.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.SerializationException;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.client.ServerOperationException;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.pdx.PdxReader;
import com.gemstone.gemfire.pdx.PdxSerializable;
import com.gemstone.gemfire.pdx.PdxWriter;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * @author dsmith
 *
 */
public class ClassNotFoundExceptionDUnitTest extends CacheTestCase {

  /**
   * @param name
   */
  public ClassNotFoundExceptionDUnitTest(String name) {
    super(name);
  }
  
  public void testDataSerializable() throws InterruptedException {
    doTest(new ObjectFactory() { public Object get() { return new ClassNotFoundDataSerializable();} });
  }
  
  public void testPdx() throws InterruptedException {
    doTest(new ObjectFactory() { public Object get() { return new ClassNotFoundPdx(false);} });
  }
  
  public void doTest(final ObjectFactory objectFactory) throws InterruptedException {
    addExpectedException("SerializationException");
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
      public void run() {
        Region region = getCache().getRegion("testSimplePdx");
        region.put("a", "b");
        region.put("b", "b");
        for(int i =0; i < 10; i++) {
          region.put(i, i);
        }
        if(!region.containsKey("test")) {
          region.put("test", objectFactory.get());
        }
        try {
          region.put(objectFactory.get(), objectFactory.get());
          fail("Should have received an exception");
        } catch(SerializationException expected) {
          //ok
        } catch(ServerOperationException expected) {
          if(!(expected.getCause() instanceof SerializationException) && !(expected.getCause() instanceof ClassNotFoundException) ) {
            throw expected;
          }
        }
//        try {
//          region.replace("test", objectFactory.get(), objectFactory.get());
//          fail("Should have received an exception");
//        } catch(SerializationException expected) {
//          //ok
//        } catch(ServerOperationException expected) {
//          if(!(expected.getCause() instanceof SerializationException) && !(expected.getCause() instanceof ClassNotFoundException)) {
//            throw expected;
//          }
//        }
      }
    };
    
    SerializableRunnable getValue = new SerializableRunnable() {
      public void run() {
        Region region = getCache().getRegion("testSimplePdx");
        try {
          assertNotNull(region.get("test"));
          fail("Should have received an exception");
        } catch(SerializationException expected) {
          //ok
        } catch(ServerOperationException expected) {
          if(!(expected.getCause() instanceof SerializationException) && !(expected.getCause() instanceof ClassNotFoundException)) {
            throw expected;
          }
        }
      }
    };
    
    SerializableRunnable registerInterest = new SerializableRunnable() {
      public void run() {
        Region region = getCache().getRegion("testSimplePdx");
        
        try {
          ArrayList keys = new ArrayList();
          for(int i =0; i < 1000; i++) {
            keys.add(i);
          }
          keys.add("test");
          region.getAll(keys);
          fail("Should have received an exception");
        } catch(SerializationException expected) {
          System.out.println("hi");
          //ok
        } catch(ServerOperationException expected) {
          if(!(expected.getCause() instanceof SerializationException) && !(expected.getCause() instanceof ClassNotFoundException)) {
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
      public Object call() throws Exception {
        AttributesFactory af = new AttributesFactory();
//        af.setScope(Scope.DISTRIBUTED_ACK);
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
      public Object call() throws Exception {
        disconnectFromDS();
        ClientCacheFactory cf = new ClientCacheFactory();
        cf.addPoolServer(getServerHostName(vm.getHost()), port);
        cf.setPoolSubscriptionEnabled(true);
        ClientCache cache = getClientCache(cf);
        cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY)
        .create("testSimplePdx");
        return null;
      }
    };
    vm.invoke(createRegion);
  }
  
  private static class ClassNotFoundDataSerializable implements DataSerializable {
    
    public ClassNotFoundDataSerializable() {
      
    }

    public void toData(DataOutput out) throws IOException {
    }

    public void fromData(DataInput in) throws IOException,
        ClassNotFoundException {
      throw new ClassNotFoundException("Test exception");
    }
  }
  
  public static class ClassNotFoundPdx implements PdxSerializable {
    
    public ClassNotFoundPdx(boolean throwIt) {
      
    }
    
    public ClassNotFoundPdx() throws ClassNotFoundException {
      throw new ClassNotFoundException("Test Exception");
    }

    public void toData(PdxWriter writer) {
      writer.writeString("field1", "string");
      
    }

    public void fromData(PdxReader reader) {
      
    }
  }
  
  private static interface ObjectFactory extends Serializable {
    public Object get();
  }

}
