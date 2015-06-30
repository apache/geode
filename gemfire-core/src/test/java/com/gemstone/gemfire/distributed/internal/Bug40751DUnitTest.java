/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal;
	
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
	
import com.gemstone.gemfire.*;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.InterestPolicy;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.SubscriptionAttributes;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.internal.cache.lru.Sizeable;
	
import dunit.Host;
import dunit.SerializableCallable;
import dunit.SerializableRunnable;
import dunit.VM;
	
public class Bug40751DUnitTest extends CacheTestCase {
	 
  public Bug40751DUnitTest(String name) {
    super(name);
  }
  
  @Override
  public void tearDown2() throws Exception {
    super.tearDown2();
    disconnectAllFromDS();
  }
	
	 
  public void testRR() {
    System.setProperty("p2p.nodirectBuffers", "true");
    try {
      Host host = Host.getHost(0);
      VM vm0 = host.getVM(1);
      VM vm1 = host.getVM(2);

      SerializableRunnable createDataRegion = new SerializableRunnable("createRegion") {
        public void run()
        {
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
        public void run()
        {
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
      invokeInEveryVM(new SerializableCallable() {
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
    props.setProperty("conserve-sockets", "true");
    //    props.setProperty("mcast-port", "12333");
    //    props.setProperty(DistributionConfig.DISABLE_TCP_NAME, "true");
    return props;
  }
	 
	 
  private static final class MyClass implements DataSerializable {
	
	   
    public MyClass() {
    }
	
	
	
    public void fromData(DataInput in) throws IOException,
                                              ClassNotFoundException {
    }
	
    public void toData(DataOutput out) throws IOException {
      throw new RuntimeException("A Fake runtime exception in toData");
    }
	
  }
}