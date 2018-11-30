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
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import org.apache.geode.distributed.internal.DMStats;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.VersionedDataStream;
import org.apache.geode.internal.shared.unsafe.ChannelBufferUnsafeDataOutputStream;
import org.apache.geode.internal.util.concurrent.StoppableReentrantLock;

/**
 * A {@link BaseMsgStreamer} implementation that directly writes to the channels
 * in a buffered manner actually streaming data without any chunking etc.
 */
public final class MsgChannelStreamer extends ChannelBufferUnsafeDataOutputStream
    implements BaseMsgStreamer, VersionedDataStream {

  private final DistributionMessage msg;
  private final boolean directReply;
  private final List<Connection> connections;
  private final StoppableReentrantLock[] locks;
  private final DMStats stats;
  private final Version remoteVersion;

  /**
   * Any exceptions that happen during sends.
   */
  private ConnectExceptions ce;

  private static final Comparator<Connection> idCompare = (c1, c2) -> {
    final long id1 = c1.getUniqueId();
    final long id2 = c2.getUniqueId();
    return (id1 < id2) ? -1 : ((id1 == id2) ? 0 : 1);
  };

  private MsgChannelStreamer(List<Connection> connections,
      Connection firstConn, DistributionMessage msg, boolean directReply,
      DMStats stats, Version remoteVersion) throws SocketException {
    // apply the VM.maxDirectMemory() limit for these on-the-fly streamers
    super(firstConn.getSocket().getChannel(),
        firstConn.getConduit().tcpBufferSize);
    this.msg = msg;
    this.directReply = directReply;
    final int numConnections = connections.size();
    // keep the connections in order sorted by localPort for consistent locking
    // of shared connections
    if (numConnections > 1) {
      connections.sort(idCompare);
    }
    this.connections = connections;
    this.locks = new StoppableReentrantLock[numConnections];
    this.stats = stats;
    this.remoteVersion = remoteVersion;
  }

  private static ArrayList<Connection> singleConnection(Connection conn) {
    ArrayList<Connection> list = new ArrayList<>(1);
    list.add(conn);
    return list;
  }

  public static BaseMsgStreamer create(List<Connection> connections,
      final DistributionMessage msg, final boolean directReply,
      DMStats stats) throws SocketException {
    // if all connections have the same version then use one MsgChannelStreamer
    Connection firstConn = connections.get(0);
    Version remoteVersion = firstConn.remoteVersion;
    boolean hasSameVersions = true;
    final int numConnections = connections.size();
    // choose firstConn for locking using some criteria (e.g. min localPort)
    for (int i = 1; i < numConnections; i++) {
      Connection conn = connections.get(i);
      if (conn.getSocket().getLocalPort() < firstConn.getSocket()
          .getLocalPort()) {
        firstConn = conn;
      }
      Version connVersion = conn.remoteVersion;
      if (remoteVersion == null) {
        if (connVersion == null || connVersion.equals(Version.CURRENT)) {
          continue;
        }
      } else if (remoteVersion.equals(connVersion)) {
        continue;
      }
      hasSameVersions = false;
      break;
    }
    if (hasSameVersions) {
      return new MsgChannelStreamer(connections, firstConn, msg,
          directReply, stats, remoteVersion);
    } else {
      // create list of streamers (separate even if there are common versions
      // to keep things simple as this case itself is a rare one)
      ArrayList<BaseMsgStreamer> streamers = new ArrayList<>(numConnections);
      for (int i = 0; i < numConnections; i++) {
        Connection conn = connections.get(i);
        streamers.add(new MsgChannelStreamer(singleConnection(conn), conn,
            msg, directReply, stats, conn.remoteVersion));
      }
      return new MsgStreamerList(streamers);
    }
  }

  @Override
  public void reserveConnections(long startTime, long ackTimeout,
      long ackSDTimeout) {
    final List<Connection> connections = this.connections;
    final int numConnections = connections.size();
    for (int i = 0; i < numConnections; i++) {
      Connection conn = connections.get(i);
      conn.setInUse(true, startTime, ackTimeout, ackSDTimeout, connections);
      if (ackTimeout > 0) {
        conn.scheduleAckTimeouts();
      }
    }
  }

  @Override
  public List<?> getSentConnections() {
    return this.connections;
  }

  @Override
  public ConnectExceptions getConnectExceptions() {
    return this.ce;
  }

  private void acquireLocks() {
    final List<Connection> connections = this.connections;
    final int numConnections = connections.size();
    for (int i = 0; i < numConnections; i++) {
      final StoppableReentrantLock lock = connections.get(i).getOutLock();
      lock.lock();
      // assign only after successful acquisition of the lock
      this.locks[i] = lock;
    }
  }

  private void releaseLocks() {
    final int numLocks = this.locks.length;
    for (int i = 0; i < numLocks; i++) {
      this.locks[i].unlock();
      this.locks[i] = null;
    }
  }

  @Override
  public int writeMessage() throws IOException {
    final long startNumBytes = this.bytesWritten;
    final DMStats stats = this.stats;
    acquireLocks();
    try {
      long start = stats.startMsgSerialization();
      byte msgType = Connection.NORMAL_MSG_TYPE;
      if (directReply) {
        msgType |= Connection.DIRECT_ACK_BIT;
      }
      putByte(msgType);
      InternalDataSerializer.writeDSFID(msg, this);
      stats.endMsgSerialization(start);
      flush();
    } finally {
      releaseLocks();
    }
    if (msg.containsRegionContentChange()) {
      final List<Connection> connections = this.connections;
      final int numConnections = connections.size();
      for (int i = 0; i < numConnections; i++) {
        connections.get(i).incMessagesSent();
      }
    }
    return (int) (this.bytesWritten - startNumBytes);
  }

  @Override
  public void close() throws IOException {
    // flush should already done in normal course but for exception
    // cases discard whatever is left in the buffer
    this.addrPosition = this.addrLimit = 0;
    releaseBuffer();
  }

  @Override
  public void release() {
    // just clear the buffer
    this.buffer.clear();
    resetBufferPositions();
  }

  @Override
  public Version getVersion() {
    return this.remoteVersion;
  }

  private int writeBufferToChannel(final ByteBuffer buffer,
      final Connection conn, final SocketChannel channel,
      final boolean flush) {
    final DMStats stats = this.stats;
    final boolean origSocketInUse = conn.socketInUse;
    byte originalState = -1;
    try {
      if (!conn.connected) {
        throw new ConnectionException("not connected to " + conn.getRemoteAddress());
      }
      synchronized (conn.stateLock) {
        originalState = conn.connectionState;
        conn.connectionState = Connection.STATE_SENDING;
      }
      conn.socketInUse = true;
      if (!conn.isSharedResource()) {
        stats.incTOSentMsg();
      }

      long startLock = stats.startSocketLock();
      stats.endSocketLock(startLock);
      long start = stats.startSocketWrite(true);
      int numWritten = 0;
      try {
        if (flush) {
          do {
            numWritten += super.writeBuffer(buffer, channel);
          } while (buffer.hasRemaining());
        } else {
          numWritten += super.writeBufferNoWait(buffer, channel);
        }
        return numWritten;
      } finally {
        stats.endSocketWrite(true, start, numWritten, 0);
      }

    } catch (IOException ex) {
      if (this.ce == null)
        this.ce = new ConnectExceptions();
      this.ce.addFailure(conn.getRemoteAddress(), ex);
      conn.closeForReconnect("closing due to unexpected IOException");
      return -1;
    } catch (ConnectionException ex) {
      if (this.ce == null)
        this.ce = new ConnectExceptions();
      this.ce.addFailure(conn.getRemoteAddress(), ex);
      conn.closeForReconnect("closing due to unexpected ConnetionException");
      return -1;
    } finally {
      conn.accessed();
      conn.socketInUse = origSocketInUse;
      synchronized (conn.stateLock) {
        conn.connectionState = originalState;
      }
    }
  }

  @Override
  protected int writeBuffer(final ByteBuffer buffer,
      WritableByteChannel channel) throws IOException {
    final List<Connection> connections = this.connections;
    final int numConnections = connections.size();
    final int bufferPosition = buffer.position();
    final int bufferSize = buffer.limit() - bufferPosition;
    if (numConnections == 1) {
      Connection conn = connections.get(0);
      if (writeBufferToChannel(buffer, conn, conn.getSocket().getChannel(),
          true) < 0) {
        connections.remove(0);
      }
      return bufferSize;
    }

    ArrayList<Connection> pendingConnections = null;
    IntArrayList pendingWritten = null;
    int numWritten;
    // write to all the channels
    for (int i = numConnections - 1; i >= 0; i--) {
      Connection conn = connections.get(i);
      SocketChannel socketChannel = conn.getSocket().getChannel();
      // rewind buffer to the start
      buffer.position(bufferPosition);
      if ((numWritten = writeBufferToChannel(buffer, conn,
          socketChannel, false)) >= 0) {
        if (numWritten < bufferSize) {
          // put into pending list and go on to next channel
          if (pendingConnections == null) {
            int initSize = numConnections >>> 1;
            pendingConnections = new ArrayList<>(initSize);
            pendingWritten = new IntArrayList(initSize);
          }
          pendingConnections.add(conn);
          pendingWritten.add(bufferPosition + numWritten);
        }
      } else {
        connections.remove(i);
      }
    }
    // now process the pending channels in a blocking manner
    if (pendingConnections != null) {
      processPendingChannels(buffer, pendingConnections, pendingWritten);
    }

    // position the buffer at the end indicating everything was written
    buffer.position(buffer.limit());

    this.bytesWritten += bufferSize;
    return bufferSize;
  }

  private void processPendingChannels(ByteBuffer buffer,
      ArrayList<Connection> pendingConnections, IntArrayList written)
      throws IOException {
    final int numChannels = pendingConnections.size();
    for (int i = 0; i < numChannels; i++) {
      final Connection conn = pendingConnections.get(i);
      // move buffer to appropriate position
      buffer.position(written.getInt(i));
      if (writeBufferToChannel(buffer, conn, conn.getSocket().getChannel(),
          true) < 0) {
        this.connections.remove(conn);
      }
    }
  }

  @Override
  public long getParkNanosMax() {
    // never throw timeout here
    return Long.MAX_VALUE;
  }

  @Override
  protected int writeBufferNoWait(ByteBuffer buffer,
      WritableByteChannel channel) throws IOException {
    // all writes are blocking for messages
    return writeBuffer(buffer, channel);
  }
}
