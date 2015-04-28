/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client.internal;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;

import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerQueueStatus;

/**
 * Represents a connection from a client to a server.
 * Instances are created, kept, and used by {@link PoolImpl}.
 * @author darrel
 * @since 5.7
 */
public interface Connection {
  public static final long DEFAULT_CONNECTION_ID = 26739;
  
  public Socket getSocket();
  public ByteBuffer getCommBuffer();
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
