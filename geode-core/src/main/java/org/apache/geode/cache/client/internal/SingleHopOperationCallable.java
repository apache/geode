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
package org.apache.geode.cache.client.internal;

import java.util.concurrent.Callable;

import org.apache.geode.cache.client.AllConnectionsInUseException;
import org.apache.geode.cache.client.internal.ExecuteRegionFunctionOp.ExecuteRegionFunctionOpImpl;
import org.apache.geode.cache.client.internal.ExecuteRegionFunctionSingleHopOp.ExecuteRegionFunctionSingleHopOpImpl;
import org.apache.geode.distributed.internal.ServerLocation;

public class SingleHopOperationCallable implements Callable {

  private final ServerLocation server;
  private final InternalPool pool;
  private final AbstractOp op;
  private final UserAttributes securityAttributes;

  public SingleHopOperationCallable(ServerLocation server, InternalPool pool, AbstractOp op,
      UserAttributes securityAttributes) {
    this.server = server;
    this.pool = pool;
    this.op = op;
    this.securityAttributes = securityAttributes;
  }

  @Override
  public Object call() throws Exception {
    op.initMessagePart();
    Object result = null;
    boolean onlyUseExistingCnx =
        ((pool.getMaxConnections() != -1 && pool.getConnectionCount() >= pool.getMaxConnections())
            ? true : false);
    op.setAllowDuplicateMetadataRefresh(!onlyUseExistingCnx);
    try {
      UserAttributes.userAttributes.set(securityAttributes);
      result = this.pool.executeOn(server, op, true, onlyUseExistingCnx);
    } catch (AllConnectionsInUseException ex) {
      // if we reached connection limit and don't have available connection to
      // that server,then execute function on one of the connections available
      // from other servers instead of creating new connection to the original
      // server
      if (op instanceof ExecuteRegionFunctionSingleHopOpImpl) {
        ExecuteRegionFunctionSingleHopOpImpl newop = (ExecuteRegionFunctionSingleHopOpImpl) op;
        result = this.pool.execute(new ExecuteRegionFunctionOpImpl(newop));
      } else {
        result = this.pool.execute(this.op);
      }
    } finally {
      UserAttributes.userAttributes.set(null);
    }
    return result;
  }

  public ServerLocation getServer() {
    return this.server;
  }

  public AbstractOp getOperation() {
    return this.op;
  }
}
