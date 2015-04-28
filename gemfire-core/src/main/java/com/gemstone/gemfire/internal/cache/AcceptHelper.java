/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache;


/**
 * @author Sudhir Menon
 *
 *
 * Helper class that holds values needed by the search optimizer to do its work.
 *
 */

import java.util.Set;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SelectionKey;
import java.io.IOException;
import java.net.*;

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

  public AcceptHelper() {
  }

  public void close() {
    try {
      if (ackServerChannel != null) {
        ackServerChannel.close();
      }
      nackServerChannel.close();
    }
    catch(IOException ioe) {
      ioe.printStackTrace();
    }
    closed = true;
  }

  public synchronized void reset(boolean ackPortInit) {
    try {
      if (ackPortInit) {
        ackServerChannel = ServerSocketChannel.open();
        ackServerChannel.configureBlocking(false);
        ackServerChannel.socket().bind(null,1);
        //ackPort = ackServerChannel.socket().getLocalPort();
        InetSocketAddress addr = (InetSocketAddress) ackServerChannel.
                                  socket().getLocalSocketAddress();
        ackPort = addr.getPort();

      }
      else {
        ackServerChannel = null;
      }

      nackServerChannel = ServerSocketChannel.open();
      nackServerChannel.configureBlocking(false);
      nackServerChannel.socket().bind(null,1);
      InetSocketAddress addr = (InetSocketAddress) nackServerChannel.
                                  socket().getLocalSocketAddress();
     // nackPort = nackServerChannel.socket().getLocalPort();
      nackPort = addr.getPort();

    }
    catch(IOException ioe) {
      ioe.printStackTrace();
    }
    catch(Exception e) {
      e.printStackTrace();
    }
    closed=false;

  }

  public boolean isClosed() {
    return closed;
  }

}
