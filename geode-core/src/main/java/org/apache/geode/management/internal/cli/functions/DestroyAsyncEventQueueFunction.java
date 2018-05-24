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
package org.apache.geode.management.internal.cli.functions;

import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueImpl;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.management.cli.CliFunction;
import org.apache.geode.management.internal.cli.commands.DestroyAsyncEventQueueCommand;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult.StatusState;

/**
 * Function used by the 'destroy async-event-queue' gfsh command to destroy an asynchronous event
 * queue on a member.
 */
public class DestroyAsyncEventQueueFunction extends CliFunction<String> {

  private static final long serialVersionUID = -7754359270344102817L;

  @Override
  public CliFunctionResult executeFunction(FunctionContext<String> context) {

    String aeqId = context.getArguments();
    String memberId = context.getMemberName();
    AsyncEventQueueImpl aeq = (AsyncEventQueueImpl) context.getCache().getAsyncEventQueue(aeqId);

    if (aeq == null) {
      return new CliFunctionResult(memberId, StatusState.IGNORABLE, String
          .format(DestroyAsyncEventQueueCommand.DESTROY_ASYNC_EVENT_QUEUE__AEQ_0_NOT_FOUND, aeqId));
    }

    aeq.stop();
    aeq.destroy();
    return new CliFunctionResult(memberId, StatusState.OK, String
        .format(DestroyAsyncEventQueueCommand.DESTROY_ASYNC_EVENT_QUEUE__AEQ_0_DESTROYED, aeqId));
  }

  @Override
  public String getId() {
    return DestroyAsyncEventQueueFunction.class.getName();
  }
}
