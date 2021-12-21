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
package org.apache.geode.internal.cache.partitioned;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.IOException;

import org.junit.Test;

import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InitialImageOperation;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.partitioned.FetchEntriesMessage.FetchEntriesReplyMessage;
import org.apache.geode.internal.cache.partitioned.FetchEntriesMessage.FetchEntriesResponse;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.test.fake.Fakes;

public class FetchEntriesMessageJUnitTest {

  private GemFireCacheImpl cache;

  private VersionTag createVersionTag(boolean validVersionTag)
      throws ClassNotFoundException, IOException {
    VersionTag tag = VersionTag.create(cache.getMyId());
    if (validVersionTag) {
      tag.setRegionVersion(1);
      tag.setEntryVersion(1);
    }
    return tag;
  }

  private HeapDataOutputStream createDummyChunk() throws IOException, ClassNotFoundException {
    HeapDataOutputStream mos =
        new HeapDataOutputStream(InitialImageOperation.CHUNK_SIZE_IN_BYTES + 2048,
            KnownVersion.CURRENT);
    mos.reset();
    DataSerializer.writeObject("keyWithOutVersionTag", mos);
    DataSerializer.writeObject("valueWithOutVersionTag", mos);
    DataSerializer.writeObject(null /* versionTag */, mos);

    DataSerializer.writeObject("keyWithVersionTag", mos);
    DataSerializer.writeObject("valueWithVersionTag", mos);

    VersionTag tag = createVersionTag(true);
    DataSerializer.writeObject(tag, mos);

    DataSerializer.writeObject(null, mos);
    return mos;
  }

  @Test
  public void testProcessChunk() throws Exception {
    cache = Fakes.cache();
    PartitionedRegion pr = mock(PartitionedRegion.class);
    InternalDistributedSystem system = cache.getInternalDistributedSystem();

    FetchEntriesResponse response = new FetchEntriesResponse(system, pr, null, 0);
    HeapDataOutputStream chunkStream = createDummyChunk();
    FetchEntriesReplyMessage reply =
        new FetchEntriesReplyMessage(null, 0, 0, chunkStream, 0, 0, 0, false, false);
    reply.chunk = chunkStream.toByteArray();
    response.processChunk(reply);
    assertNull(response.returnRVV);
    assertEquals(2, response.returnValue.size());
    assertTrue(response.returnValue.get("keyWithOutVersionTag").equals("valueWithOutVersionTag"));
    assertTrue(response.returnValue.get("keyWithVersionTag").equals("valueWithVersionTag"));
    assertNull(response.returnVersions.get("keyWithOutVersionTag"));
    assertNotNull(response.returnVersions.get("keyWithVersionTag"));
  }
}
