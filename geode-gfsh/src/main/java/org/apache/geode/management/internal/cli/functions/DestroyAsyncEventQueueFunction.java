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
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.internal.cache.xmlcache.CacheXml;
import org.apache.geode.management.internal.cli.commands.DestroyAsyncEventQueueCommand;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.management.internal.functions.CliFunctionResult;

/**
 * Function used by the 'destroy async-event-queue' gfsh command to destroy an asynchronous event
 * queue on a member.
 */
public class DestroyAsyncEventQueueFunction
    implements InternalFunction<DestroyAsyncEventQueueFunctionArgs> {

  private static final long serialVersionUID = -7754359270344102817L;

  @Override
  public void execute(FunctionContext<DestroyAsyncEventQueueFunctionArgs> context) {
    DestroyAsyncEventQueueFunctionArgs aeqArgs =
        context.getArguments();
    String aeqId = aeqArgs.getId();
    String memberId = context.getMemberName();

    try {
      AsyncEventQueueImpl aeq = (AsyncEventQueueImpl) context.getCache().getAsyncEventQueue(aeqId);
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
        // this is the XmlEntity that needs to be removed from the cluster config
        XmlEntity xmlEntity = getAEQXmlEntity("id", aeqId);

        aeq.stop();
        aeq.destroy();

        @SuppressWarnings("deprecation")
        final CliFunctionResult lastResult =
            new CliFunctionResult(memberId, xmlEntity, String.format(
                DestroyAsyncEventQueueCommand.DESTROY_ASYNC_EVENT_QUEUE__AEQ_0_DESTROYED, aeqId));
        context.getResultSender()
            .lastResult(lastResult);
      }
    } catch (Exception e) {
      context.getResultSender().lastResult(new CliFunctionResult(memberId, e, e.getMessage()));
    }
  }

  XmlEntity getAEQXmlEntity(String key, String value) {
    return new XmlEntity(CacheXml.ASYNC_EVENT_QUEUE, key, value);
  }

  @Override
  public String getId() {
    return DestroyAsyncEventQueueFunction.class.getName();
  }
}
