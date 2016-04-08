/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * Pivotal Additions:
 * Removed classifications
 * Removed method transferToFully
 * 
 */
package com.gemstone.gemfire.internal;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.WritableByteChannel;

/**
 * This implements an output stream that can have a timeout while writing.
 * This sets non-blocking flag on the socket channel.
 * So after creating this object , read() on 
 * {@link Socket#getInputStream()} and write() on 
 * {@link Socket#getOutputStream()} on the associated socket will throw 
 * llegalBlockingModeException.
 * Please use {@link SocketInputStream} for reading.
 */
public class SocketOutputStream extends OutputStream 
                                implements WritableByteChannel {                                
  
  private Writer writer;
  
  private static class Writer extends SocketIOWithTimeout {
    WritableByteChannel channel;
    
    Writer(WritableByteChannel channel, long timeout) throws IOException {
      super((SelectableChannel)channel, timeout);
      this.channel = channel;
    }
    
    @Override
    int performIO(ByteBuffer buf) throws IOException {
      return channel.write(buf);
    }
  }
  
  /**
   * Create a new ouput stream with the given timeout. If the timeout
   * is zero, it will be treated as infinite timeout. The socket's
   * channel will be configured to be non-blocking.
   * 
   * @param channel 
   *        Channel for writing, should also be a {@link SelectableChannel}.  
   *        The channel will be configured to be non-blocking.
   * @param timeout timeout in milliseconds. must not be negative.
   * @throws IOException
   */
  public SocketOutputStream(WritableByteChannel channel, long timeout) 
                                                         throws IOException {
    SocketIOWithTimeout.checkChannelValidity(channel);
    writer = new Writer(channel, timeout);
  }
  
  /**
   * Same as SocketOutputStream(socket.getChannel(), timeout):<br><br>
   * 
   * Create a new ouput stream with the given timeout. If the timeout
   * is zero, it will be treated as infinite timeout. The socket's
   * channel will be configured to be non-blocking.
   * 
   * @see SocketOutputStream#SocketOutputStream(WritableByteChannel, long)
   *  
   * @param socket should have a channel associated with it.
   * @param timeout timeout timeout in milliseconds. must not be negative.
   * @throws IOException
   */
  public SocketOutputStream(Socket socket, long timeout) 
                                         throws IOException {
    this(socket.getChannel(), timeout);
  }
  
  @Override
  public void write(int b) throws IOException {
    /* If we need to, we can optimize this allocation.
     * probably no need to optimize or encourage single byte writes.
     */
    byte[] buf = new byte[1];
    buf[0] = (byte)b;
    write(buf, 0, 1);
  }
  
  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    ByteBuffer buf = ByteBuffer.wrap(b, off, len);
    while (buf.hasRemaining()) {
      try {
        if (write(buf) < 0) {
          throw new IOException("The stream is closed");
        }
      } catch (IOException e) {
        /* Unlike read, write can not inform user of partial writes.
         * So will close this if there was a partial write.
         */
        if (buf.capacity() > buf.remaining()) {
          writer.close();
        }
        throw e;
      }
    }
  }

  @Override
  public synchronized void close() throws IOException {
    /* close the channel since Socket.getOuputStream().close() 
     * closes the socket.
     */
    writer.channel.close();
    writer.close();
  }

  /**
   * Returns underlying channel used by this stream.
   * This is useful in certain cases like channel for 
   * {@link FileChannel#transferTo(long, long, WritableByteChannel)}
   */
  public WritableByteChannel getChannel() {
    return writer.channel; 
  }

  //WritableByteChannle interface 
  
  @Override
  public boolean isOpen() {
    return writer.isOpen();
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    return writer.doIO(src, SelectionKey.OP_WRITE);
  }
  
  /**
   * waits for the underlying channel to be ready for writing.
   * The timeout specified for this stream applies to this wait.
   *
   * @throws SocketTimeoutException 
   *         if select on the channel times out.
   * @throws IOException
   *         if any other I/O error occurs. 
   */
  public void waitForWritable() throws IOException {
    writer.waitForIO(SelectionKey.OP_WRITE);
  }

  public void setTimeout(int timeoutMs) {
    writer.setTimeout(timeoutMs);
  }
}