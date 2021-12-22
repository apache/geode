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
package org.apache.geode;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_SAMPLING_ENABLED;
import static org.junit.Assert.fail;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Properties;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.InternalInstantiator;

/**
 * This class makes sure that instantatiors are persisted to disk and can be recovered.
 */
@SuppressWarnings("deprecation")
public class DiskInstantiatorsJUnitTest {

  private DistributedSystem ds = null;
  private Cache c = null;
  private Region r = null;

  @BeforeClass
  public static void beforeClass() {
    Instantiator.register(new Instantiator(Payload.class, (byte) 22) {
      @Override
      public DataSerializable newInstance() {
        return new Payload();
      }
    });
    Instantiator.register(new Instantiator(Key.class, (byte) 21) {
      @Override
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

    ds = DistributedSystem.connect(cfg);
    c = CacheFactory.create(ds);
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    factory.setDiskSynchronous(true);
    factory.setDiskStoreName(
        c.createDiskStoreFactory().create("DiskInstantiatorsJUnitTest").getName());

    r = c.createRegion("DiskInstantiatorsJUnitTest", factory.create());
  }

  private void disconnect() throws CacheException {
    r = null;
    if (c != null) {
      c.close();
      c = null;
    }
    if (ds != null) {
      ds.disconnect();
      ds = null;
    }
  }

  @Test
  public void testDiskInstantiators() throws CacheException {
    try {
      connect();
      int size = r.entrySet(false).size();
      if (size != 0) {
        fail("expected 0 entries but had " + size);
      }
      ds.getLogWriter().info("adding entry");
      r.put(new Key(1), new Payload(100));
      disconnect();
      // now unregister and make sure we can restore
      InternalInstantiator.unregister(Payload.class, (byte) 22);
      InternalInstantiator.unregister(Key.class, (byte) 21);
      connect();
      size = r.entrySet(false).size();
      if (size != 1) {
        fail("expected 1 entry but had " + size);
      }
      Object value = r.get(new Key(1));
      ds.getLogWriter().info("found entry");
      if (!(value instanceof Payload)) {
        fail("Expected value to be an instance of Payload but it was " + value.getClass());
      }
      disconnect();
    } finally {
      try {
        if (ds == null) {
          connect();
        }
        if (r != null) {
          ds.getLogWriter().info("destroying region");
          r.localDestroyRegion();
        }
      } finally {
        disconnect();
      }
    }
  }

  private static class Payload implements DataSerializable {
    private byte[] data;

    public Payload() {}

    public Payload(int size) {
      data = new byte[size];
    }

    @Override
    public void toData(DataOutput dataOutput) throws IOException {
      DataSerializer.writeByteArray(data, dataOutput);
    }

    @Override
    public void fromData(DataInput dataInput) throws IOException {
      data = DataSerializer.readByteArray(dataInput);
    }
  }
  private static class Key implements DataSerializable {
    public int hashCode() {
      return key.hashCode();
    }

    public boolean equals(Object obj) {
      if (obj instanceof Key) {
        return key.equals(((Key) obj).key);
      } else {
        return false;
      }
    }

    private Long key;

    public Key() {}

    public Key(long k) {
      key = k;
    }

    @Override
    public void toData(DataOutput dataOutput) throws IOException {
      dataOutput.writeLong(key);
    }

    @Override
    public void fromData(DataInput dataInput) throws IOException {
      key = dataInput.readLong();
    }
  }
}
