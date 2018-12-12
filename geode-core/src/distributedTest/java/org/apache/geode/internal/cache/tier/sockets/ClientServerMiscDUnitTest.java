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

package org.apache.geode.internal.cache.tier.sockets;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;

public class ClientServerMiscDUnitTest extends ClientServerMiscDUnitTestBase {


  /**
   * Ensure that an Invalidate event that
   */
  @Test
  public void testInvalidateOnInvalidEntryInServerReachesClient() throws Exception {
    VM server = VM.getVM(0);
    String regionPath = Region.SEPARATOR + REGION_NAME2;
    PORT1 = server.invoke(() -> {
      int port = createServerCache(true, -1, false);
      getCache().getRegion(regionPath).put(server_k1, "VALUE1");
      getCache().getRegion(regionPath).invalidate(server_k1);
      return port;
    });
    createClientCache(NetworkUtils.getServerHostName(), PORT1);
    registerInterest();
    Region region = static_cache.getRegion(regionPath);
    assertThat(region.containsKey(server_k1)).isTrue();
    assertThat(region.get(server_k1)).isNull();

    System.out.println("do it with a forceEntry==false code path");
    RegionEntry entry = ((LocalRegion) region).getRegionEntry(server_k1);
    int entryVersion = entry.getVersionStamp().getEntryVersion();
    server.invoke(() -> {
      // getCache().getRegion(regionPath).invalidate(server_k1);

      // create a "remote" invalidateion event and invalidate the already-invalid entry
      LocalRegion localRegion = (LocalRegion) getCache().getRegion(regionPath);
      VersionTag tag = localRegion.getRegionEntry(server_k1).getVersionStamp().asVersionTag();
      InternalDistributedMember id = localRegion.getMyId();
      tag.setMemberID(new InternalDistributedMember(id.getInetAddress(), id.getPort() + 1));
      tag.setEntryVersion(tag.getEntryVersion() + 1);
      tag.setEntryVersion(5);
      tag.setIsRemoteForTesting();
      EntryEventImpl event =
          EntryEventImpl.create(localRegion, Operation.INVALIDATE, server_k1, null,
              null, false, id);
      EventID eventID = new EventID(new byte[100], 1, 1);
      event.setVersionTag(tag);
      event.setEventId(eventID);
      localRegion.getRegionMap().invalidate(event, false, false, false);
    });
    await()
        .until(() -> entry.getVersionStamp().getEntryVersion() > entryVersion);

    System.out.println("do it again with a forceEntry==true code path");
    RegionEntry entry2 = ((LocalRegion) region).getRegionEntry(server_k1);
    int entryVersion2 = entry.getVersionStamp().getEntryVersion();
    server.invoke(() -> {
      // create a "remote" invalidateion event and invalidate the already-invalid entry
      LocalRegion localRegion = (LocalRegion) getCache().getRegion(regionPath);
      VersionTag tag = localRegion.getRegionEntry(server_k1).getVersionStamp().asVersionTag();
      InternalDistributedMember id = localRegion.getMyId();
      tag.setMemberID(new InternalDistributedMember(id.getInetAddress(), id.getPort() + 1));
      tag.setEntryVersion(tag.getEntryVersion() + 1);
      tag.setEntryVersion(6);
      tag.setIsRemoteForTesting();
      EntryEventImpl event =
          EntryEventImpl.create(localRegion, Operation.INVALIDATE, server_k1, null,
              null, false, id);
      EventID eventID = new EventID(new byte[100], 1, 2);
      event.setVersionTag(tag);
      event.setEventId(eventID);
      localRegion.getRegionMap().invalidate(event, false, true, false);
    });
    await()
        .until(() -> entry2.getVersionStamp().getEntryVersion() > entryVersion2);
  }
}
