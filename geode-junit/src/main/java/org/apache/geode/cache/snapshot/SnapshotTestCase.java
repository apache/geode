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

import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import com.examples.snapshot.MyObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.snapshot.RegionGenerator.SerializationType;

public class SnapshotTestCase {
  private File snapshotDirectory;
  protected Cache cache;
  RegionGenerator regionGenerator;
  DiskStore diskStore;

  @Rule
  public TemporaryFolder baseDir = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    File storeDirectory = baseDir.newFolder("store");
    snapshotDirectory = baseDir.newFolder("snapshots");

    regionGenerator = new RegionGenerator();

    CacheFactory cf = new CacheFactory().set(MCAST_PORT, "0").set(LOG_LEVEL, "error");
    cache = cf.create();

    diskStore = cache.createDiskStoreFactory().setMaxOplogSize(1)
        .setDiskDirs(new File[] {storeDirectory}).create("snapshotTest");
  }

  @After
  public void tearDown() throws Exception {
    cache.close();
  }

  File getSnapshotDirectory() {
    return snapshotDirectory;
  }

  Map<Integer, MyObject> createExpected(SerializationType type) {
    Map<Integer, MyObject> expected = new HashMap<>();
    for (int i = 0; i < 100; i++) {
      expected.put(i, regionGenerator.createData(type, i, "The number is " + i));
    }
    return expected;
  }
}
