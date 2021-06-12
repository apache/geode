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

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.CliFunction;
import org.apache.geode.management.internal.functions.CliFunctionResult;

public class ResumeAsyncEventQueueDispatcherFunction extends CliFunction {
  private static final String ID =
      "org.apache.geode.management.internal.cli.functions.ResumeAsyncEventQueueDispatcherFunction";

  @Override
  public String getId() {
    return ID;
  }

  @Override
  public CliFunctionResult executeFunction(FunctionContext context) {
    String AEQId = (String) context.getArguments();

    Cache cache = context.getCache();
    DistributedMember member = cache.getDistributedSystem().getDistributedMember();

    AsyncEventQueue queue = cache.getAsyncEventQueue(AEQId);

    if (queue == null) {
      return new CliFunctionResult(member.getId(), CliFunctionResult.StatusState.ERROR,
          "Async Event Queue \"" + AEQId +
              " cannot be found");
    }

    if (queue.isDispatchingPaused()) {
      queue.resumeEventDispatching();

      return new CliFunctionResult(member.getId(), CliFunctionResult.StatusState.OK,
          "Async Event Queue \"" + AEQId
              + "\" dispatching was resumed successfully");
    } else {
      return new CliFunctionResult(member.getId(), CliFunctionResult.StatusState.OK,
          "Async Event Queue \"" + AEQId
              + "\" dispatching was not paused.");
    }


  }
}
