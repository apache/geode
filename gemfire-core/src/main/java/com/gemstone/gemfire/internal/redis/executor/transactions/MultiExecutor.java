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
package com.gemstone.gemfire.internal.redis.executor.transactions;

import com.gemstone.gemfire.cache.CacheTransactionManager;
import com.gemstone.gemfire.cache.TransactionId;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisConstants;

public class MultiExecutor extends TransactionExecutor {

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {

    CacheTransactionManager txm = context.getCacheTransactionManager();

    command.setResponse(Coder.getSimpleStringResponse(context.getByteBufAllocator(), "OK"));

    if (context.hasTransaction()) {
      throw new IllegalStateException(RedisConstants.ERROR_NESTED_MULTI);
    }

    txm.begin();

    TransactionId id = txm.suspend();

    context.setTransactionID(id);

  }

}
