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
package org.apache.geode.distributed.internal;


import org.apache.geode.cache.GemFireCache;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.tcpserver.TcpHandler;
import org.apache.geode.distributed.internal.tcpserver.TcpServer;

/**
 * A handler which responds to messages for the {@link TcpServer}
 *
 * @since GemFire 5.7
 */
public interface RestartableTcpHandler extends TcpHandler {

  /**
   * Informs the handler that TcpServer is restarting with the given distributed system and cache
   *
   * @param sharedConfig TODO
   */
  void restarting(DistributedSystem ds, GemFireCache cache,
      InternalConfigurationPersistenceService sharedConfig);

  /**
   * Informs the handler that restart has completed
   */
  default void restartCompleted(DistributedSystem ds) {}

}
