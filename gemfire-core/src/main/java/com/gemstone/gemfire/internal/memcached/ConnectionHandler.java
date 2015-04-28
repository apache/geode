/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.memcached;

import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.internal.memcached.commands.ClientError;
import com.gemstone.gemfire.memcached.GemFireMemcachedServer;
import com.gemstone.gemfire.memcached.GemFireMemcachedServer.Protocol;

/**
 * One instance of ConnectionHandle is created for each
 * client that connects to {@link GemFireMemcachedServer}
 * and is responsible for reading requests and sending
 * responses to this client.
 * 
 * 
 * @author Swapnil Bawaskar
 *
 */
public class ConnectionHandler implements Runnable {

  private final Socket socket;
  
  private final Cache cache;
  
  private final Protocol protocol;

  private static LogWriter logger;
  
  public ConnectionHandler(Socket socket, Cache cache, Protocol protocol) {
    this.socket = socket;
    this.cache = cache;
    this.protocol = protocol;
    if (logger == null) {
      logger = this.cache.getLogger();
    }
  }
  
  public void run() {
    RequestReader request = new RequestReader(this.socket, this.protocol);
    while(!Thread.currentThread().isInterrupted()) {
      try {
        Command command = request.readCommand();
        if (logger.fineEnabled()) {
          logger.fine("processing command:"+command);
        }
        ByteBuffer reply = command.getCommandProcessor().processCommand(
            request, this.protocol, cache);
        if (reply != null) {
          request.sendReply(reply);
        }
        if (command == Command.QUIT || command == Command.QUITQ) {
          socket.close();
          break;
        }
      } catch (ClientError e) {
        request.sendException(e);
      } catch (IllegalArgumentException e) {
        // thrown by Command.valueOf() when there is no matching command
        request.sendException(e);
      } catch (CacheClosedException cc) {
        Thread.currentThread().interrupt();
      } catch (IOException e) {
        Thread.currentThread().interrupt();
      }
    }
    logger.fine("Connection handler "+Thread.currentThread().getName()+" terminating");
  }

  protected static LogWriter getLogger() {
    return logger;
  }
}
