/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client.internal;

import java.util.List;

import java.util.concurrent.ScheduledExecutorService;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;

/**
 * @author dsmith
 * @since 5.7
 * 
 */
public interface QueueManager {
  
  public QueueConnections getAllConnectionsNoWait();

  public QueueConnections getAllConnections();

  void start(ScheduledExecutorService background);

  void close(boolean keepAlive);
  
  public static interface QueueConnections {
    Connection getPrimary();
    List/*<Connection>*/ getBackups();
    QueueConnectionImpl getConnection(Endpoint endpoint);
  }

  public QueueState getState();

  public InternalPool getPool();
  
  public InternalLogWriter getSecurityLogger();

  public void readyForEvents(InternalDistributedSystem system);

  public void emergencyClose();
  
  public void checkEndpoint(ClientUpdater qc, Endpoint endpoint);
}
