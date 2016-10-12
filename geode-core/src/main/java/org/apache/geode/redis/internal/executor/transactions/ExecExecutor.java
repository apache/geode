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
package org.apache.geode.redis.internal.executor.transactions;

import io.netty.buffer.ByteBuf;

import java.util.Queue;

import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.CommitConflictException;
import org.apache.geode.cache.TransactionId;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisConstants;

public class ExecExecutor extends TransactionExecutor {

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {

    CacheTransactionManager txm = context.getCacheTransactionManager();

    if (!context.hasTransaction()) {
      command.setResponse(Coder.getNilResponse(context.getByteBufAllocator()));
      return;
    }

    TransactionId transactionId = context.getTransactionID();

    txm.resume(transactionId);

    boolean hasError = hasError(context.getTransactionQueue());

    if (hasError)
      txm.rollback();
    else {
      try {
        txm.commit();
      } catch (CommitConflictException e) {
        command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(),
            RedisConstants.ERROR_COMMIT_CONFLICT));
        context.clearTransaction();
        return;
      }
    }

    ByteBuf response = constructResponseExec(context);
    command.setResponse(response);

    context.clearTransaction();
  }

  private ByteBuf constructResponseExec(ExecutionHandlerContext context) {
    Queue<Command> cQ = context.getTransactionQueue();
    ByteBuf response = context.getByteBufAllocator().buffer();
    response.writeByte(Coder.ARRAY_ID);
    response.writeBytes(Coder.intToBytes(cQ.size()));
    response.writeBytes(Coder.CRLFar);

    for (Command c : cQ) {
      ByteBuf r = c.getResponse();
      response.writeBytes(r);
    }
    return response;
  }

  private boolean hasError(Queue<Command> queue) {
    for (Command c : queue) {
      if (c.hasError())
        return true;
    }
    return false;
  }
}
