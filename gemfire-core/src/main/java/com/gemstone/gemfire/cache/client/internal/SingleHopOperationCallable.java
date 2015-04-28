/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client.internal;

import java.util.concurrent.Callable;

import com.gemstone.gemfire.cache.client.AllConnectionsInUseException;
import com.gemstone.gemfire.cache.client.internal.ExecuteRegionFunctionOp.ExecuteRegionFunctionOpImpl;
import com.gemstone.gemfire.cache.client.internal.ExecuteRegionFunctionSingleHopOp.ExecuteRegionFunctionSingleHopOpImpl;
import com.gemstone.gemfire.distributed.internal.ServerLocation;
/**
 * 
 * @author ymahajan
 *
 */
public class SingleHopOperationCallable implements Callable {

  final private ServerLocation server;

  final private PoolImpl pool;

  final private AbstractOp op;

  final private UserAttributes securityAttributes;

  public SingleHopOperationCallable(ServerLocation server, PoolImpl pool,
      AbstractOp op, UserAttributes securityAttributes) {
    this.server = server;
    this.pool = pool;
    this.op = op;
    this.securityAttributes = securityAttributes;
  }

  public Object call() throws Exception {
    op.initMessagePart();
    Object result = null;
    boolean onlyUseExistingCnx = ((pool.getMaxConnections() != -1 && pool
        .getConnectionCount() >= pool.getMaxConnections()) ? true : false);
    try {
      UserAttributes.userAttributes.set(securityAttributes);
      result = this.pool.executeOn(server, op, true, onlyUseExistingCnx);
    }
    catch (AllConnectionsInUseException ex) {
      // if we reached connection limit and don't have available connection to
      // that server,then execute function on one of the connections available
      // from other servers instead of creating new connection to the original
      // server
      if (op instanceof ExecuteRegionFunctionSingleHopOpImpl){
        ExecuteRegionFunctionSingleHopOpImpl newop = (ExecuteRegionFunctionSingleHopOpImpl)op;
        result = this.pool.execute(new ExecuteRegionFunctionOpImpl(newop));
      }else {
        result = this.pool.execute(this.op);
      }
    }
    finally {
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

