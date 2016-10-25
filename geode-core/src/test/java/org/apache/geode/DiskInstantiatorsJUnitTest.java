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
package org.apache.geode;

import org.apache.geode.cache.*;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.InternalInstantiator;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Properties;

import static org.apache.geode.distributed.ConfigurationProperties.*;

import static org.junit.Assert.fail;

/**
 * This class makes sure that instantatiors are persisted to disk
 * and can be recovered.
 */
@SuppressWarnings("deprecation")
@Category(IntegrationTest.class)
public class DiskInstantiatorsJUnitTest {

  private DistributedSystem ds = null;
  private Cache c = null;
  private Region r = null;

  @BeforeClass
  public static void beforeClass() {
    Instantiator.register(new Instantiator(Payload.class, (byte) 22) {
      public DataSerializable newInstance() {
        return new Payload();
      }
    });
    Instantiator.register(new Instantiator(Key.class, (byte) 21) {
      public DataSerializable newInstance() {
        return new Key();
      }
    });
  }

  @AfterClass
  public static void afterClass() {
    InternalInstantiator.unregister(Payload.class, (byte) 22);
    InternalInstantiator.unregister(Key.class, (byte) 21);
  }
  
  @After
  public void after() {
    disconnect();
  }
  
  private void connect() throws CacheException {
    Properties cfg = new Properties();
    cfg.setProperty(MCAST_PORT, "0");
    cfg.setProperty(LOCATORS, "");
    cfg.setProperty(STATISTIC_SAMPLING_ENABLED, "false");

    this.ds = DistributedSystem.connect(cfg);
    this.c = CacheFactory.create(ds);
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    factory.setDiskSynchronous(true);
    factory.setDiskStoreName(this.c.createDiskStoreFactory()
                             .create("DiskInstantiatorsJUnitTest")
                             .getName());
      
    this.r = this.c.createRegion("DiskInstantiatorsJUnitTest",
                                   factory.create());
  }
  
  private void disconnect() throws CacheException {
    this.r = null;
    if (this.c != null) {
      this.c.close();
      this.c = null;
    }
    if (this.ds != null) {
      this.ds.disconnect();
      this.ds = null;
    }
  }
  
  @Test
  public void testDiskInstantiators() throws CacheException {
    try {
      connect();
      int size = this.r.entries(false).size();
      if (size != 0) {
        fail("expected 0 entries but had " + size);
      }
      this.ds.getLogWriter().info("adding entry");
      r.put(new Key(1), new Payload(100));
      disconnect();
      // now unregister and make sure we can restore
      InternalInstantiator.unregister(Payload.class, (byte)22);
      InternalInstantiator.unregister(Key.class, (byte)21);
      connect();
      size = this.r.entries(false).size();
      if (size != 1) {
        fail("expected 1 entry but had " + size);
      }
      Object value = r.get(new Key(1));
      this.ds.getLogWriter().info("found entry");
      if (!(value instanceof Payload)) {
        fail("Expected value to be an instance of Payload but it was "
             + value.getClass());
      }
      disconnect();
    } finally {
      try {
        if (this.ds == null) {
          connect();
        }
        if (this.r != null) {
          this.ds.getLogWriter().info("destroying region");
          this.r.localDestroyRegion();
        }
      } finally {
        disconnect();
      }
    }
  }

  private static class Payload implements DataSerializable {
    private byte[] data;
    public Payload() {
    }
    public Payload(int size) {
      this.data = new byte[size];
    }
    public void toData(DataOutput dataOutput) throws IOException  {
      DataSerializer.writeByteArray(this.data, dataOutput);
    }
    public void fromData(DataInput dataInput) throws IOException {
      this.data = DataSerializer.readByteArray(dataInput);
    }
  }
  private static class Key implements DataSerializable {
    public int hashCode() {
      return this.key.hashCode();
    }
    public boolean equals(Object obj) {
      if (obj instanceof Key) {
        return this.key.equals(((Key)obj).key);
      } else {
        return false;
      }
    }
    private Long key;
    public Key() {
    }
    public Key(long k) {
      this.key = new Long(k);
    }
    public void toData(DataOutput dataOutput) throws IOException  {
      dataOutput.writeLong(this.key.longValue());
    }
    public void fromData(DataInput dataInput) throws IOException {
      this.key = new Long(dataInput.readLong());
    }
  }
}
