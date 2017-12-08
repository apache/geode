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
package org.apache.geode.cache.snapshot;

import com.examples.snapshot.MyDataSerializer;
import com.examples.snapshot.MyDataSerializer.MyObjectDataSerializable2;
import com.examples.snapshot.MyObject;
import com.examples.snapshot.MyObjectDataSerializable;
import com.examples.snapshot.MyObjectPdx;
import com.examples.snapshot.MyObjectPdx.MyEnumPdx;
import com.examples.snapshot.MyPdxSerializer.MyObjectPdx2;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;

public class RegionGenerator {
  public enum RegionType {
    REPLICATE,
    REPLICATE_PERSISTENT,
    REPLICATE_PERSISTENT_OVERFLOW,
    PARTITION,
    PARTITION_PERSISTENT,
    PARTITION_PERSISTENT_OVERFLOW;

    public static RegionType[] persistentValues() {
      return new RegionType[] {REPLICATE_PERSISTENT, REPLICATE_PERSISTENT_OVERFLOW,
          PARTITION_PERSISTENT, PARTITION_PERSISTENT_OVERFLOW};
    }
  }

  public enum SerializationType {
    SERIALIZABLE, DATA_SERIALIZABLE, DATA_SERIALIZER, PDX, PDX_SERIALIZER;

    public static SerializationType[] offlineValues() {
      return new SerializationType[] {SERIALIZABLE, DATA_SERIALIZABLE, DATA_SERIALIZER};
    }
  }

  public RegionGenerator() {
    DataSerializer.register(MyDataSerializer.class);
  }

  public <K, V> Region<K, V> createRegion(Cache c, String diskStore, RegionType type, String name) {
    Region<Integer, MyObject> r = c.getRegion(name);
    switch (type) {
      case REPLICATE:
        return c.<K, V>createRegionFactory(RegionShortcut.REPLICATE).create(name);

      case REPLICATE_PERSISTENT:
        return c.<K, V>createRegionFactory(RegionShortcut.REPLICATE_PERSISTENT)
            .setDiskStoreName(diskStore).create(name);

      case REPLICATE_PERSISTENT_OVERFLOW:
        return c.<K, V>createRegionFactory(RegionShortcut.REPLICATE_PERSISTENT_OVERFLOW)
            .setEvictionAttributes(
                EvictionAttributes.createLIFOEntryAttributes(1, EvictionAction.OVERFLOW_TO_DISK))
            .setDiskStoreName(diskStore).create(name);

      case PARTITION:
        return c.<K, V>createRegionFactory(RegionShortcut.PARTITION).create(name);

      case PARTITION_PERSISTENT:
        return c.<K, V>createRegionFactory(RegionShortcut.PARTITION_PERSISTENT)
            .setDiskStoreName(diskStore).create(name);

      case PARTITION_PERSISTENT_OVERFLOW:
        return c.<K, V>createRegionFactory(RegionShortcut.PARTITION_PERSISTENT_OVERFLOW)
            .setEvictionAttributes(
                EvictionAttributes.createLIFOEntryAttributes(5, EvictionAction.OVERFLOW_TO_DISK))
            .setDiskStoreName(diskStore).create(name);
    }
    throw new IllegalArgumentException();
  }

  public MyObject createData(SerializationType type, int f1, String f2) {
    switch (type) {
      case SERIALIZABLE:
        return new MyObject(f1, f2);

      case DATA_SERIALIZABLE:
        return new MyObjectDataSerializable(f1, f2);

      case DATA_SERIALIZER:
        return new MyObjectDataSerializable2(f1, f2);

      case PDX:
        return new MyObjectPdx(f1, f2, MyEnumPdx.const1);

      case PDX_SERIALIZER:
        return new MyObjectPdx2(f1, f2);
    }
    throw new IllegalArgumentException();
  }
}
