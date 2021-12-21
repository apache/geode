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
package org.apache.geode.internal.cache.ha;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.InetAddress;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.ha.ThreadIdentifier.WanType;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class ThreadIdentifierJUnitTest {

  @Test
  public void testEqualsIgnoresUUIDBytes() throws Exception {
    InternalDistributedMember id = new InternalDistributedMember(InetAddress.getLocalHost(), 1234);
    id.setVersionForTest(KnownVersion.GFE_90);
    byte[] memberIdBytes = EventID.getMembershipId(new ClientProxyMembershipID(id));
    byte[] memberIdBytesWithoutUUID = new byte[memberIdBytes.length - (2 * 8 + 1)];// UUID bytes +
                                                                                   // weight byte
    System.arraycopy(memberIdBytes, 0, memberIdBytesWithoutUUID, 0,
        memberIdBytesWithoutUUID.length);
    ThreadIdentifier threadIdWithUUID = new ThreadIdentifier(memberIdBytes, 1);
    ThreadIdentifier threadIdWithoutUUID = new ThreadIdentifier(memberIdBytesWithoutUUID, 1);
    assertEquals(threadIdWithoutUUID, threadIdWithUUID);
    assertEquals(threadIdWithUUID, threadIdWithoutUUID);
    assertEquals(threadIdWithoutUUID.hashCode(), threadIdWithUUID.hashCode());

    EventID eventIDWithUUID = new EventID(memberIdBytes, 1, 1);
    EventID eventIDWithoutUUID = new EventID(memberIdBytesWithoutUUID, 1, 1);
    assertEquals(eventIDWithUUID, eventIDWithoutUUID);
    assertEquals(eventIDWithoutUUID, eventIDWithUUID);
    assertEquals(eventIDWithoutUUID.hashCode(), eventIDWithUUID.hashCode());
  }

  @Test
  public void testPutAllId() {
    int id = 42;
    int bucketNumber = 113;

    long putAll = ThreadIdentifier.createFakeThreadIDForBulkOp(bucketNumber, id);

    assertTrue(ThreadIdentifier.isPutAllFakeThreadID(putAll));
    assertEquals(42, ThreadIdentifier.getRealThreadID(putAll));
  }

  @Test
  public void testWanId() {
    int id = 42;

    long wan1 = ThreadIdentifier.createFakeThreadIDForParallelGSPrimaryBucket(1, id, 0);
    assertEquals(42, ThreadIdentifier.getRealThreadID(wan1));
    assertTrue(ThreadIdentifier.isParallelWANThreadID(wan1));
    {
      long real_tid_with_wan = ThreadIdentifier.getRealThreadIDIncludingWan(wan1);
      assertEquals(42, ThreadIdentifier.getRealThreadID(real_tid_with_wan));
      assertTrue(ThreadIdentifier.isParallelWANThreadID(real_tid_with_wan));
      assertTrue(WanType.matches(real_tid_with_wan));
    }

    long wan2 = ThreadIdentifier.createFakeThreadIDForParallelGSSecondaryBucket(1, id, 0);
    assertEquals(42, ThreadIdentifier.getRealThreadID(wan2));
    assertTrue(ThreadIdentifier.isParallelWANThreadID(wan2));
    {
      long real_tid_with_wan = ThreadIdentifier.getRealThreadIDIncludingWan(wan2);
      assertEquals(42, ThreadIdentifier.getRealThreadID(real_tid_with_wan));
      assertTrue(ThreadIdentifier.isParallelWANThreadID(real_tid_with_wan));
      assertTrue(WanType.matches(real_tid_with_wan));
    }

    long wan3 = ThreadIdentifier.createFakeThreadIDForParallelGateway(1, id, 0);
    assertEquals(42, ThreadIdentifier.getRealThreadID(wan3));
    assertTrue(ThreadIdentifier.isParallelWANThreadID(wan3));
    {
      long real_tid_with_wan = ThreadIdentifier.getRealThreadIDIncludingWan(wan3);
      assertEquals(42, ThreadIdentifier.getRealThreadID(real_tid_with_wan));
      assertTrue(ThreadIdentifier.isParallelWANThreadID(real_tid_with_wan));
      assertTrue(WanType.matches(real_tid_with_wan));
    }
  }

  @Test
  public void testWanAndPutAllId() {
    int id = 42;
    int bucketNumber = 113;

    long putAll = ThreadIdentifier.createFakeThreadIDForBulkOp(bucketNumber, id);

    long wan1 = ThreadIdentifier.createFakeThreadIDForParallelGSPrimaryBucket(1, putAll, 0);
    assertEquals(42, ThreadIdentifier.getRealThreadID(wan1));
    assertTrue(ThreadIdentifier.isParallelWANThreadID(wan1));
    assertTrue(ThreadIdentifier.isPutAllFakeThreadID(wan1));
    {
      long real_tid_with_wan = ThreadIdentifier.getRealThreadIDIncludingWan(wan1);
      assertEquals(42, ThreadIdentifier.getRealThreadID(real_tid_with_wan));
      assertTrue(ThreadIdentifier.isParallelWANThreadID(real_tid_with_wan));
      assertTrue(WanType.matches(real_tid_with_wan));
    }

    long wan2 = ThreadIdentifier.createFakeThreadIDForParallelGSSecondaryBucket(1, putAll, 0);
    assertEquals(42, ThreadIdentifier.getRealThreadID(wan2));
    assertTrue(ThreadIdentifier.isParallelWANThreadID(wan2));
    assertTrue(ThreadIdentifier.isPutAllFakeThreadID(wan2));
    {
      long real_tid_with_wan = ThreadIdentifier.getRealThreadIDIncludingWan(wan2);
      assertEquals(42, ThreadIdentifier.getRealThreadID(real_tid_with_wan));
      assertTrue(ThreadIdentifier.isParallelWANThreadID(real_tid_with_wan));
      assertTrue(WanType.matches(real_tid_with_wan));
    }

    long wan3 = ThreadIdentifier.createFakeThreadIDForParallelGateway(1, putAll, 0);
    assertEquals(42, ThreadIdentifier.getRealThreadID(wan3));
    assertTrue(ThreadIdentifier.isParallelWANThreadID(wan3));
    assertTrue(ThreadIdentifier.isPutAllFakeThreadID(wan3));
    {
      long real_tid_with_wan = ThreadIdentifier.getRealThreadIDIncludingWan(wan3);
      assertEquals(42, ThreadIdentifier.getRealThreadID(real_tid_with_wan));
      assertTrue(ThreadIdentifier.isParallelWANThreadID(real_tid_with_wan));
      assertTrue(WanType.matches(real_tid_with_wan));
    }

    long tid = 4054000001L;
    assertTrue(ThreadIdentifier.isParallelWANThreadID(tid));
    assertFalse(ThreadIdentifier.isParallelWANThreadID(putAll));
  }
}
