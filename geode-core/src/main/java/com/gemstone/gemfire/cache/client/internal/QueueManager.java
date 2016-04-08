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
package com.gemstone.gemfire.cache.client.internal;

import java.util.List;

import java.util.concurrent.ScheduledExecutorService;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;

/**
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
