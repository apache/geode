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
package org.apache.geode.cache.client.internal;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;

import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.tier.sockets.ServerQueueStatus;

/**
 * Represents a connection from a client to a server.
 * Instances are created, kept, and used by {@link PoolImpl}.
 * @since GemFire 5.7
 */
public interface Connection {
  public static final long DEFAULT_CONNECTION_ID = 26739;
  
  public Socket getSocket();
  public ByteBuffer getCommBuffer() throws SocketException;
  public ConnectionStats getStats();
  /**
   * Forcefully close the resources used by this connection.
   * This should be called if the connection or the server dies.
   */
  public void destroy();

  /**
   * Return true if this connection has been destroyed
   */
  public boolean isDestroyed();

  /**
   * Gracefully close the connection by notifying 
   * the server. It is not necessary to call destroy
   * after closing the connection.
   * @param keepAlive What do do this server to
   * client connection proxy on this server. 
   * @throws Exception if there was an error notifying the server.
   * The connection will still be destroyed.
   */
  public void close(boolean keepAlive) throws Exception;
  
  public ServerLocation getServer();
  
  public Endpoint getEndpoint();
  
  public ServerQueueStatus getQueueStatus();
  
  public Object execute(Op op) throws Exception;

  public void emergencyClose();
  
  public short getWanSiteVersion();
   
  public void setWanSiteVersion(short wanSiteVersion);
  
  public int getDistributedSystemId();
  
  public OutputStream getOutputStream();
  
  public InputStream getInputStream();

  public void setConnectionID(long id);

  public long getConnectionID();
}
