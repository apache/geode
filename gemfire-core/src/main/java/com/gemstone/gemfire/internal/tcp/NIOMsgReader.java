/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.tcp;

import java.io.EOFException;
import java.io.IOException;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import com.gemstone.gemfire.internal.Version;

/**
 * A message reader which reads from the socket
 * using (blocking) nio.
 * @author dsmith
 *
 */
public class NIOMsgReader extends MsgReader {
  
  /** the buffer used for NIO message receipt */
  private ByteBuffer nioInputBuffer;
  private final SocketChannel inputChannel;
  private int lastReadPosition;
  private int lastProcessedPosition; 
  
  public NIOMsgReader(Connection conn, Version version) throws SocketException {
    super(conn, version);
    this.inputChannel = conn.getSocket().getChannel();
  }


  @Override
  public ByteBuffer readAtLeast(int bytes) throws IOException {
    ensureCapacity(bytes);
    
    while(lastReadPosition - lastProcessedPosition < bytes) {
      nioInputBuffer.limit(nioInputBuffer.capacity());
      nioInputBuffer.position(lastReadPosition);
      int bytesRead = inputChannel.read(nioInputBuffer);
      if(bytesRead < 0) {
        throw new EOFException();
      }
      lastReadPosition = nioInputBuffer.position();
    }
    nioInputBuffer.limit(lastProcessedPosition + bytes);
    nioInputBuffer.position(lastProcessedPosition);
    lastProcessedPosition = nioInputBuffer.limit();

    return nioInputBuffer;
  }
  
  /** gets the buffer for receiving message length bytes */
  protected void ensureCapacity(int bufferSize) {
    //Ok, so we have a buffer that's big enough
    if(nioInputBuffer != null && nioInputBuffer.capacity() > bufferSize) {
      if(nioInputBuffer.capacity() - lastProcessedPosition < bufferSize) {
        nioInputBuffer.limit(lastReadPosition);
        nioInputBuffer.position(lastProcessedPosition);
        nioInputBuffer.compact();
        lastReadPosition = nioInputBuffer.position();
        lastProcessedPosition = 0;
      }
      return;
    }
    
    //otherwise, we have no buffer to a buffer that's too small
    
    if (nioInputBuffer == null) {
      int allocSize = conn.getReceiveBufferSize();
      if (allocSize == -1) {
        allocSize = conn.owner.getConduit().tcpBufferSize;
      }
      if(allocSize > bufferSize) {
        bufferSize = allocSize;
      }
    }
    ByteBuffer oldBuffer = nioInputBuffer;
    nioInputBuffer = Buffers.acquireReceiveBuffer(bufferSize, getStats());
    
    if(oldBuffer != null) {
      oldBuffer.limit(lastReadPosition);
      oldBuffer.position(lastProcessedPosition);
      nioInputBuffer.put(oldBuffer);
      lastReadPosition = nioInputBuffer.position(); // fix for 45064
      lastProcessedPosition = 0;
      Buffers.releaseReceiveBuffer(oldBuffer, getStats());
    }
  }

  @Override
  public void close() {
    ByteBuffer tmp = this.nioInputBuffer;
    if(tmp != null) {
      this.nioInputBuffer = null;
      Buffers.releaseReceiveBuffer(tmp, getStats());
    }
  }
}
