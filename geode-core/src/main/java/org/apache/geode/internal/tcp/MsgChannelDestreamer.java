/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package org.apache.geode.internal.tcp;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;

import org.apache.geode.internal.Version;
import org.apache.geode.internal.VersionedDataStream;
import org.apache.geode.internal.shared.unsafe.ChannelBufferUnsafeDataInputStream;

/**
 * Direct streaming buffered reads from TCP channels.
 */
public final class MsgChannelDestreamer
    extends ChannelBufferUnsafeDataInputStream
    implements VersionedDataStream {

  private final Connection conn;
  private Version remoteVersion;
  private long newMessageStart;
  private boolean newMessage;

  MsgChannelDestreamer(Connection conn, SocketChannel channel, int bufferSize) {
    super(channel, bufferSize);
    this.conn = conn;
  }

  void setRemoteVersion(Version version) {
    this.remoteVersion = version;
  }

  void startNewMessage() {
    this.newMessageStart = this.bytesRead;
    this.newMessage = true;
  }

  int getLastMessageLength() {
    return (int) (this.bytesRead - this.newMessageStart);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Version getVersion() {
    return this.remoteVersion;
  }

  @Override
  protected int refillBuffer(final ByteBuffer channelBuffer,
      final int tryReadBytes, final String eofMessage) throws IOException {
    final byte originalState;
    synchronized (conn.stateLock) {
      originalState = conn.connectionState;
      if (originalState == Connection.STATE_IDLE) {
        conn.connectionState = Connection.STATE_READING;
      }
    }
    final int numBytes = super.refillBuffer(channelBuffer,
        tryReadBytes, eofMessage);
    if (originalState == Connection.STATE_IDLE) {
      synchronized (conn.stateLock) {
        conn.connectionState = Connection.STATE_IDLE;
      }
    }
    return numBytes;
  }

  @Override
  protected int readIntoBuffer(ByteBuffer buffer) throws IOException {
    if (!conn.connected) {
      throw new ClosedChannelException();
    }
    int numBytes = super.readIntoBuffer(buffer);
    if (numBytes > 0) {
      conn.getConduit().getStats().incMessagesBeingReceived(
          this.newMessage, numBytes);
      this.newMessage = false;
    }
    return numBytes;
  }

  @Override
  public long getParkNanosMax() {
    // increased timeout to enable detection of failed sockets
    return 90000000000L;
  }

  @Override
  protected int readIntoBufferNoWait(ByteBuffer buffer)
      throws IOException {
    return readIntoBuffer(buffer);
  }
}
