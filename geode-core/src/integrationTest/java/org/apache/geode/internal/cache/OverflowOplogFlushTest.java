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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

/**
 * Testing recovery from failures writing OverflowOplog entries
 */
public class OverflowOplogFlushTest extends DiskRegionTestingBase {
  @Test
  public void testAsyncChannelWriteRetriesOnFailureDuringFlush() throws Exception {
    OverflowOplog oplog = getOverflowOplog();
    int numberOfWriteFailures = 1;
    doChannelFlushWithFailures(oplog, numberOfWriteFailures);
  }

  @Test
  public void testChannelWriteRetriesOnFailureDuringFlush() throws Exception {
    OverflowOplog oplog = getOverflowOplog();
    int numberOfWriteFailures = 1;
    doChannelFlushWithFailures(oplog, numberOfWriteFailures);
  }

  @Test
  public void testChannelRecoversFromWriteFailureRepeatedRetriesDuringFlush() throws Exception {
    OverflowOplog oplog = getOverflowOplog();
    int numberOfWriteFailures = 3;
    doChannelFlushWithFailures(oplog, numberOfWriteFailures);
  }

  @Test
  public void testOplogFlushThrowsIOExceptionWhenNumberOfChannelWriteRetriesExceedsLimit() {
    OverflowOplog oplog = getOverflowOplog();
    int numberOfFailures = 6; // exceeds the retry limit in Oplog
    assertThatThrownBy(() -> doChannelFlushWithFailures(oplog, numberOfFailures))
        .isInstanceOf(IOException.class);
  }

  @Test
  public void testOverflowOplogByteArrayFlush() throws Exception {
    OverflowOplog oplog = getOverflowOplog();
    doPartialChannelByteArrayFlushForOverflowOpLog(oplog);
  }

  private OverflowOplog getOverflowOplog() {
    DiskRegionProperties props = new DiskRegionProperties();
    props.setOverFlowCapacity(1);
    region = DiskRegionHelperFactory.getSyncOverFlowOnlyRegion(cache, props);
    region.put("K1", "v1"); // add two entries to make it overflow
    region.put("K2", "v2");
    DiskRegion dr = ((LocalRegion) region).getDiskRegion();
    OverflowOplog oplog = dr.getDiskStore().overflowOplogs.getActiveOverflowOplog();
    assertThat(oplog)
        .as("oplog")
        .isNotNull();
    return oplog;
  }

  private void doChannelFlushWithFailures(OverflowOplog oplog, int numFailures) throws IOException {
    AtomicInteger numberOfRemainingFailures = new AtomicInteger(numFailures);
    FileChannel fileChannelThatFails = new FileChannelWrapper(oplog.getFileChannel()) {
      @Override
      public int write(ByteBuffer buffer) throws IOException {
        if (numberOfRemainingFailures.get() > 0) {
          // Force channel.write() failure
          buffer.position(buffer.limit());
          numberOfRemainingFailures.getAndDecrement();
          return 0;
        }
        return delegate.write(buffer);
      }
    };
    oplog.testSetCrfChannel(fileChannelThatFails);

    byte[] entry1 = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19};
    byte[] entry2 = {100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115,
        116, 117, 118, 119};

    ByteBuffer oplogWriteBuffer = oplog.getWriteBuf();
    try {
      FileChannel fileChannel = oplog.getFileChannel();
      long chStartPos = fileChannel.position();
      oplogWriteBuffer.clear();
      oplogWriteBuffer.put(entry1);
      oplog.flush();

      // Write the 2nd entry without forced channel failures
      numberOfRemainingFailures.set(0);
      oplogWriteBuffer = oplog.getWriteBuf();
      oplogWriteBuffer.clear();
      oplogWriteBuffer.put(entry2);
      oplog.flush();
      long chEndPos = fileChannel.position();
      assertThat(chEndPos - chStartPos)
          .as("change in channel position")
          .isEqualTo(entry1.length + entry2.length);
      ByteBuffer dst = ByteBuffer.allocateDirect(entry1.length);
      fileChannel.position(chStartPos);
      fileChannel.read(dst);
      verifyBB(dst, entry1);
    } finally {
      region.destroyRegion();
    }
  }

  private void doPartialChannelByteArrayFlushForOverflowOpLog(OverflowOplog oplog)
      throws IOException {
    AtomicInteger numberOfFakeWrites = new AtomicInteger();
    FileChannel fileChannelThatFails = new FileChannelWrapper(oplog.getFileChannel()) {
      // Pretend to write partial data from each buffer.
      @Override
      public long write(ByteBuffer[] buffers, int offset, int length) {
        numberOfFakeWrites.incrementAndGet();
        for (ByteBuffer buffer : buffers) {
          int bufferPosition = buffer.position();
          int bufferLimit = buffer.limit();
          int halfOfBufferLimit = bufferLimit / 2;
          if (bufferPosition <= 0) {
            buffer.position(halfOfBufferLimit);
            return halfOfBufferLimit;
          } else if (bufferPosition == halfOfBufferLimit) {
            buffer.position(bufferLimit);
            return bufferLimit - halfOfBufferLimit;
          }
        }
        return 0;
      }
    };
    oplog.testSetCrfChannel(fileChannelThatFails);

    byte[] entry1 = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19};
    byte[] entry2 = {100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115,
        116, 117, 118, 119};

    ByteBuffer entry1Buffer = ByteBuffer.allocate(entry1.length).put(entry1);
    ByteBuffer entry2Buffer = ByteBuffer.allocate(entry2.length).put(entry2);

    try {
      entry2Buffer.flip();
      oplog.flush(entry1Buffer, entry2Buffer);
      assertThat(numberOfFakeWrites)
          .as("number of incomplete flush calls")
          .hasValue(4);

    } finally {
      region.destroyRegion();
    }
  }

  private static void verifyBB(ByteBuffer bb, byte[] src) {
    bb.flip();
    for (int i = 0; i < src.length; ++i) {
      assertThat(bb.get())
          .as("byte expected at position " + i)
          .isEqualTo(src[i]);
    }
  }

  static class FileChannelWrapper extends FileChannel {
    protected final FileChannel delegate;

    FileChannelWrapper(FileChannel delegate) {
      this.delegate = delegate;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
      return delegate.read(dst);
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
      return delegate.read(dsts, offset, length);
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
      return delegate.read(src);
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
      return delegate.write(srcs, offset, length);
    }

    @Override
    public long position() throws IOException {
      return delegate.position();
    }

    @Override
    public FileChannel position(long newPosition) throws IOException {
      return delegate.position(newPosition);
    }

    @Override
    public long size() throws IOException {
      return delegate.size();
    }

    @Override
    public FileChannel truncate(long size) throws IOException {
      return delegate.truncate(size);
    }

    @Override
    public void force(boolean metaData) throws IOException {
      delegate.force(metaData);
    }

    @Override
    public long transferTo(long position, long count, WritableByteChannel target)
        throws IOException {
      return delegate.transferTo(position, count, target);
    }

    @Override
    public long transferFrom(ReadableByteChannel src, long position, long count)
        throws IOException {
      return delegate.transferFrom(src, position, count);
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
      return delegate.read(dst, position);
    }

    @Override
    public int write(ByteBuffer src, long position) throws IOException {
      return delegate.write(src, position);
    }

    @Override
    public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException {
      return delegate.map(mode, position, size);
    }

    @Override
    public FileLock lock(long position, long size, boolean shared) throws IOException {
      return delegate.lock(position, size, shared);
    }

    @Override
    public FileLock tryLock(long position, long size, boolean shared) throws IOException {
      return delegate.tryLock(position, size, shared);
    }

    @Override
    protected void implCloseChannel() throws IOException {
      delegate.close();
    }
  }
}
