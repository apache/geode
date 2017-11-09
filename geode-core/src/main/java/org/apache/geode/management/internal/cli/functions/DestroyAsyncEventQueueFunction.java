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

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueImpl;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.cli.commands.DestroyAsyncEventQueueCommand;

/**
 * Function used by the 'create async-event-queue' gfsh command to create an asynchronous event
 * queue on a member.
 *
 * @since GemFire 8.0
 */
public class DestroyAsyncEventQueueFunction extends FunctionAdapter implements InternalEntity {

  private static final long serialVersionUID = -7754359270344102817L;

  @SuppressWarnings("deprecation")
  @Override
  public void execute(FunctionContext context) {
    String memberId = "";

    DestroyAsyncEventQueueFunctionArgs aeqArgs =
        (DestroyAsyncEventQueueFunctionArgs) context.getArguments();
    String aeqId = aeqArgs.getId();

    InternalCache cache = (InternalCache) context.getCache();

    DistributedMember member = cache.getDistributedSystem().getDistributedMember();

    memberId = member.getId();
    // If they set a name use it instead
    if (!member.getName().equals("")) {
      memberId = member.getName();
    }

    try {
      AsyncEventQueueImpl aeq = (AsyncEventQueueImpl) cache.getAsyncEventQueue(aeqId);
      if (aeq == null) {
        if (aeqArgs.isIfExists()) {
          context.getResultSender()
              .lastResult(new CliFunctionResult(memberId, true,
                  String.format(
                      "Skipping: "
                          + DestroyAsyncEventQueueCommand.DESTROY_ASYNC_EVENT_QUEUE__AEQ_0_NOT_FOUND,
                      aeqId)));
        } else {
          context.getResultSender().lastResult(new CliFunctionResult(memberId, false, String.format(
              DestroyAsyncEventQueueCommand.DESTROY_ASYNC_EVENT_QUEUE__AEQ_0_NOT_FOUND, aeqId)));
        }
      } else {
        aeq.stop();
        aeq.destroy();
        context.getResultSender().lastResult(new CliFunctionResult(memberId, true, String.format(
            DestroyAsyncEventQueueCommand.DESTROY_ASYNC_EVENT_QUEUE__AEQ_0_DESTROYED, aeqId)));
      }
    } catch (Exception e) {
      e.printStackTrace();
      context.getResultSender().lastResult(new CliFunctionResult(memberId, e, ""));
    }
  }

  @Override
  public String getId() {
    return DestroyAsyncEventQueueFunction.class.getName();
  }
}
