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
package org.apache.geode.redis.internal.executor.transactions;

import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.TransactionId;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisConstants;

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
