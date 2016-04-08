/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
