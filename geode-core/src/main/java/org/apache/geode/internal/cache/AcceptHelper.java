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


/*
 *
 * Helper class that holds values needed by the search optimizer to do its work.
 *
 */

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.util.Set;

class AcceptHelper {
  public ServerSocketChannel ackServerChannel;
  public ServerSocketChannel nackServerChannel;
  public int ackPort = 0;
  public int nackPort = 0;;
  public Set relevantIdSet;
  public SearchLoadAndWriteProcessor processor;
  public SelectionKey ackSelKey;
  public SelectionKey nackSelKey;
  public boolean closed = false;

  public AcceptHelper() {}

  public void close() {
    try {
      if (ackServerChannel != null) {
        ackServerChannel.close();
      }
      nackServerChannel.close();
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
    closed = true;
  }

  public synchronized void reset(boolean ackPortInit) {
    try {
      if (ackPortInit) {
        ackServerChannel = ServerSocketChannel.open();
        ackServerChannel.configureBlocking(false);
        ackServerChannel.socket().bind(null, 1);
        // ackPort = ackServerChannel.socket().getLocalPort();
        InetSocketAddress addr =
            (InetSocketAddress) ackServerChannel.socket().getLocalSocketAddress();
        ackPort = addr.getPort();

      } else {
        ackServerChannel = null;
      }

      nackServerChannel = ServerSocketChannel.open();
      nackServerChannel.configureBlocking(false);
      nackServerChannel.socket().bind(null, 1);
      InetSocketAddress addr =
          (InetSocketAddress) nackServerChannel.socket().getLocalSocketAddress();
      // nackPort = nackServerChannel.socket().getLocalPort();
      nackPort = addr.getPort();

    } catch (IOException ioe) {
      ioe.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    }
    closed = false;

  }

  public boolean isClosed() {
    return closed;
  }

}
