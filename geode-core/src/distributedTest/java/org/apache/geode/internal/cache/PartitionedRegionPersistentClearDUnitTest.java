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
package org.apache.geode.internal.cache;



import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;

public class PartitionedRegionPersistentClearDUnitTest extends PartitionedRegionClearDUnitTest {

  protected RegionShortcut getRegionShortCut() {
    return RegionShortcut.PARTITION_REDUNDANT_PERSISTENT;
  }

  @Test
  public void clearFromDataStoreWithWriterOnDataStoreAndMemberShutdown() {
    configureServers(true, true);
    client1.invoke(this::initClientCache);
    client2.invoke(this::initClientCache);

    accessor.invoke(() -> feed(false));
    verifyServerRegionSize(NUM_ENTRIES);

//    dataStore3.invokeAsync(() -> getRegion(false).clear());
//    dataStore3.invoke(() -> {
//      while (((PartitionedRegion) getRegion(false)).isRegionClearInProgress()
//          && getRegion(false).size() != 0) {
//        Thread.sleep(100);
//      }
//    });
    // dataStore2.stop();

    verifyServerRegionSize(0);

    // do the region destroy to compare that the same callbacks will be triggered
    dataStore3.invoke(() -> {
      Region region = getRegion(false);
      region.destroyRegion();
    });

    assertThat(dataStore1.invoke(getWriterDestroys)).isEqualTo(dataStore1.invoke(getWriterClears))
        .isEqualTo(0);
    assertThat(dataStore3.invoke(getWriterDestroys)).isEqualTo(dataStore3.invoke(getWriterClears))
        .isEqualTo(1);
    assertThat(accessor.invoke(getWriterDestroys)).isEqualTo(accessor.invoke(getWriterClears))
        .isEqualTo(0);

    assertThat(dataStore3.invoke(getBucketRegionWriterDestroys))
        .isEqualTo(dataStore3.invoke(getBucketRegionWriterClears))
        .isEqualTo(0);
  }

  @Test
  public void clearFromDataStoreWithWriterOnDataStoreAndMemberRestart() {
    configureServers(true, true);
    client1.invoke(this::initClientCache);
    client2.invoke(this::initClientCache);

    accessor.invoke(() -> feed(false));
    verifyServerRegionSize(NUM_ENTRIES);

    dataStore3.invoke(() -> getRegion(false).clear());
//    dataStore3.invoke(() -> {
//      while (((PartitionedRegion) getRegion(false)).isRegionClearInProgress()
//          && getRegion(false).size() != 0) {
//        Thread.sleep(100);
//      }
//    });

    // dataStore2.stop();
    // dataStore2 = cluster.startServerVM(2, getProperties(), locatorPort);

    verifyServerRegionSize(0);

    // do the region destroy to compare that the same callbacks will be triggered
    dataStore3.invoke(() -> {
      Region region = getRegion(false);
      region.destroyRegion();
    });

    assertThat(dataStore1.invoke(getWriterDestroys)).isEqualTo(dataStore1.invoke(getWriterClears))
        .isEqualTo(0);
    assertThat(dataStore3.invoke(getWriterDestroys)).isEqualTo(dataStore3.invoke(getWriterClears))
        .isEqualTo(1);
    assertThat(accessor.invoke(getWriterDestroys)).isEqualTo(accessor.invoke(getWriterClears))
        .isEqualTo(0);

    assertThat(dataStore3.invoke(getBucketRegionWriterDestroys))
        .isEqualTo(dataStore3.invoke(getBucketRegionWriterClears))
        .isEqualTo(0);
  }
}
