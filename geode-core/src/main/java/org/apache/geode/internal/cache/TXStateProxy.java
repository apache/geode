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

import org.apache.geode.cache.client.internal.ServerRegionDataAccess;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.tx.TransactionalOperation.ServerRegionOperation;

/**
 * This interface extends {@link TXStateInterface} providing for a proxy for the real transaction on
 * a remote data store node.
 *
 */
public interface TXStateProxy extends TXStateInterface {

  void checkJTA(String errmsg) throws IllegalStateException;

  void setIsJTA(boolean isJTA);

  TXId getTxId();

  TXManagerImpl getTxMgr();

  void setLocalTXState(TXStateInterface state);

  void setTarget(DistributedMember target);

  DistributedMember getTarget();

  boolean isCommitOnBehalfOfRemoteStub();

  boolean setCommitOnBehalfOfRemoteStub(boolean requestedByOwner);

  boolean isOnBehalfOfClient();

  boolean isJCATransaction();

  void setJCATransaction();

  /**
   * Perform additional tasks required by the proxy to suspend a transaction
   */
  @Override
  void suspend();

  /**
   * Perform additional tasks required by the proxy to resume a transaction
   */
  @Override
  void resume();

  /**
   * record a client-side transactional operation for possible later replay
   */
  @Override
  void recordTXOperation(ServerRegionDataAccess proxy, ServerRegionOperation op, Object key,
      Object[] arguments);

  /**
   * @return the number of operations performed in this transaction
   */
  int operationCount();

  /**
   * During client transaction failover, it is possible to get two Commit (rollback) requests for a
   * single transaction. It becomes necessary to set the progress flag when the second request
   * arrives. When the requeset is processed, progress flag must be reset. see bug 43350
   *
   */
  void setInProgress(boolean progress);

  void updateProxyServer(InternalDistributedMember proxy);

  InternalDistributedMember getOnBehalfOfClientMember();
}
