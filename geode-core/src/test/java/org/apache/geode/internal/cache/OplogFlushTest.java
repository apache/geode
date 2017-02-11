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

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.*;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.geode.cache.DiskAccessException;
import org.apache.geode.internal.cache.persistence.UninterruptibleFileChannel;
import org.apache.geode.test.junit.categories.IntegrationTest;

/**
 * Testing recovery from failures writing Oplog entries
 */
@Category(IntegrationTest.class)
public class OplogFlushTest extends DiskRegionTestingBase {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  // How many times to fake the write failures
  private int nFakeChannelWrites = 0;
  private Oplog ol = null;
  private ByteBuffer bb1 = null;
  private ByteBuffer bb2 = null;
  private ByteBuffer[] bbArray = new ByteBuffer[2];
  private UninterruptibleFileChannel ch;
  private UninterruptibleFileChannel spyCh;

  @Rule
  public TestName name = new TestName();

  class FakeChannelWriteBB implements Answer<Integer> {

    @Override
    public Integer answer(InvocationOnMock invocation) throws Throwable {
      return fakeWriteBB(ol, bb1);
    }
  }

  private int fakeWriteBB(Oplog ol, ByteBuffer bb) throws IOException {
    if (nFakeChannelWrites > 0) {
      bb.position(bb.limit());
      --nFakeChannelWrites;
      return 0;
    }
    doCallRealMethod().when(spyCh).write(bb);
    return spyCh.write(bb);
  }

  private void verifyBB(ByteBuffer bb, byte[] src) {
    bb.flip();
    for (int i = 0; i < src.length; ++i) {
      assertEquals("Channel contents does not match expected at index " + i, src[i], bb.get());
    }
  }

  private void doChannelFlushWithFailures(Oplog[] oplogs, int numFailures) throws IOException {
    nFakeChannelWrites = numFailures;
    ol = oplogs[0];
    ch = ol.getFileChannel();
    spyCh = spy(ch);
    ol.testSetCrfChannel(spyCh);

    byte[] entry1 = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19};
    byte[] entry2 = {100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115,
        116, 117, 118, 119};

    bb1 = ol.getWriteBuf();
    try {
      // Force channel.write() failures when writing the first entry
      doAnswer(new FakeChannelWriteBB()).when(spyCh).write(bb1);
      long chStartPos = ol.getFileChannel().position();
      bb1.clear();
      bb1.put(entry1);
      ol.flushAll(true);

      // Write the 2nd entry without forced channel failures
      nFakeChannelWrites = 0;
      bb1 = ol.getWriteBuf();
      bb1.clear();
      bb1.put(entry2);
      ol.flushAll(true);
      long chEndPos = ol.getFileChannel().position();
      assertEquals("Change in channel position does not equal the size of the data flushed",
          entry1.length + entry2.length, chEndPos - chStartPos);
      ByteBuffer dst = ByteBuffer.allocateDirect(entry1.length);
      ol.getFileChannel().position(chStartPos);
      ol.getFileChannel().read(dst);
      verifyBB(dst, entry1);
    } finally {
      region.destroyRegion();
    }
  }

  @Test
  public void testAsyncChannelWriteRetriesOnFailureDuringFlush() throws Exception {
    region = DiskRegionHelperFactory.getAsyncOverFlowAndPersistRegion(cache, null);
    DiskRegion dr = ((LocalRegion) region).getDiskRegion();
    Oplog[] oplogs = dr.getDiskStore().persistentOplogs.getAllOplogs();
    assertNotNull("Unexpected null Oplog[] for " + dr.getName(), oplogs);
    assertNotNull("Unexpected null Oplog", oplogs[0]);

    doChannelFlushWithFailures(oplogs, 1 /* write failure */);
  }

  @Test
  public void testChannelWriteRetriesOnFailureDuringFlush() throws Exception {
    region = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache, null);
    DiskRegion dr = ((LocalRegion) region).getDiskRegion();
    Oplog[] oplogs = dr.getDiskStore().persistentOplogs.getAllOplogs();
    assertNotNull("Unexpected null Oplog[] for " + dr.getName(), oplogs);
    assertNotNull("Unexpected null Oplog", oplogs[0]);

    doChannelFlushWithFailures(oplogs, 1 /* write failure */);
  }

  @Test
  public void testChannelRecoversFromWriteFailureRepeatedRetriesDuringFlush() throws Exception {
    region = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache, null);
    DiskRegion dr = ((LocalRegion) region).getDiskRegion();
    Oplog[] oplogs = dr.getDiskStore().persistentOplogs.getAllOplogs();
    assertNotNull("Unexpected null Oplog[] for " + dr.getName(), oplogs);
    assertNotNull("Unexpected null Oplog", oplogs[0]);

    doChannelFlushWithFailures(oplogs, 3 /* write failures */);
  }

  @Test
  public void testOplogFlushThrowsIOExceptioniWhenNumberOfChannelWriteRetriesExceedsLimit()
      throws Exception {
    expectedException.expect(DiskAccessException.class);
    expectedException.expectCause(instanceOf(IOException.class));
    region = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache, null);
    DiskRegion dr = ((LocalRegion) region).getDiskRegion();
    Oplog[] oplogs = dr.getDiskStore().persistentOplogs.getAllOplogs();
    assertNotNull("Unexpected null Oplog[] for " + dr.getName(), oplogs);
    assertNotNull("Unexpected null Oplog", oplogs[0]);

    doChannelFlushWithFailures(oplogs, 6 /* exceeds the retry limit in Oplog */);
  }
}
