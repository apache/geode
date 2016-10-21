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
package org.apache.geode.distributed.internal;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.DataSerializable;
import org.apache.geode.ToDataException;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class Bug40751DUnitTest extends JUnit4CacheTestCase {

  @Override
  public final void postTearDownCacheTestCase() throws Exception {
    disconnectAllFromDS();
  }

  @Test
  public void testRR() {
    System.setProperty("p2p.nodirectBuffers", "true");
    try {
      Host host = Host.getHost(0);
      VM vm0 = host.getVM(1);
      VM vm1 = host.getVM(2);

      SerializableRunnable createDataRegion = new SerializableRunnable("createRegion") {
        public void run() {
          Cache cache = getCache();
          AttributesFactory attr = new AttributesFactory();
          attr.setScope(Scope.DISTRIBUTED_ACK);
          attr.setDataPolicy(DataPolicy.REPLICATE);
          attr.setMulticastEnabled(true);
          cache.createRegion("region1", attr.create());
        }
      };

      vm0.invoke(createDataRegion);

      SerializableRunnable createEmptyRegion = new SerializableRunnable("createRegion") {
        public void run() {
          Cache cache = getCache();
          AttributesFactory attr = new AttributesFactory();
          attr.setScope(Scope.DISTRIBUTED_ACK);
          attr.setDataPolicy(DataPolicy.EMPTY);
          Region region = cache.createRegion("region1", attr.create());
          try {
            region.put("A", new MyClass());
            fail("expected ToDataException");
          } catch (ToDataException ex) {
            if (!(ex.getCause() instanceof RuntimeException)) {
              fail("expected RuntimeException instead of " + ex.getCause());
            }
          }
        }
      };

      vm1.invoke(createEmptyRegion);
    } finally {
      Invoke.invokeInEveryVM(new SerializableCallable() {
        public Object call() throws Exception {
          System.getProperties().remove("p2p.oldIO");
          System.getProperties().remove("p2p.nodirectBuffers");
          return null;
        }
      });
      System.getProperties().remove("p2p.oldIO");
      System.getProperties().remove("p2p.nodirectBuffers");
    }
  }


  @Override
  public Properties getDistributedSystemProperties() {
    Properties props = new Properties();
    System.setProperty("p2p.oldIO", "true");
    props.setProperty(CONSERVE_SOCKETS, "true");
    // props.setProperty(DistributionConfig.ConfigurationProperties.MCAST_PORT, "12333");
    // props.setProperty(DistributionConfig.DISABLE_TCP_NAME, "true");
    return props;
  }


  private static final class MyClass implements DataSerializable {


    public MyClass() {}



    public void fromData(DataInput in) throws IOException, ClassNotFoundException {}

    public void toData(DataOutput out) throws IOException {
      throw new RuntimeException("A Fake runtime exception in toData");
    }

  }
}
