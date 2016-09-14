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
package org.apache.geode.management.internal.cli.functions;

import java.util.Properties;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.LogWriter;
import org.apache.geode.SystemFailure;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.internal.cache.xmlcache.Declarable2;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.internal.cli.domain.AsyncEventQueueDetails;

/**
 * An implementation of GemFire Function interface used to determine all the
 * async event queues that exist for the entire cache, distributed across the
 * GemFire distributed system. </p>
 * 
 * @since GemFire 8.0
 */
public class ListAsyncEventQueuesFunction extends FunctionAdapter implements InternalEntity {
  private static final Logger logger = LogService.getLogger();
  
  private static final long serialVersionUID = 1L;

  @Override
  public String getId() {
    return getClass().getName();
  }

  protected Cache getCache() {
    return CacheFactory.getAnyInstance();
  }

  @Override
  public void execute(final FunctionContext context) {
    // Declared here so that it's available when returning a Throwable
    String memberId = "";

    try {
      Cache cache = CacheFactory.getAnyInstance();

      DistributedMember member = cache.getDistributedSystem().getDistributedMember();

      memberId = member.getId();
      // If they set a name use it instead
      if (!member.getName().equals("")) {
        memberId = member.getName();
      }

      Set<AsyncEventQueue> asyncEventQueues = cache.getAsyncEventQueues();

      AsyncEventQueueDetails[] asyncEventQueueDetails = new AsyncEventQueueDetails[asyncEventQueues.size()];
      int i = 0;
      for (AsyncEventQueue queue : asyncEventQueues) {
        AsyncEventListener listener = queue.getAsyncEventListener();
        Properties listenerProperties = new Properties();
        if (listener instanceof Declarable2) {
          listenerProperties = ((Declarable2) listener).getConfig();
        }
        asyncEventQueueDetails[i++] = new AsyncEventQueueDetails(queue.getId(), queue.getBatchSize(), queue.isPersistent(), queue
            .getDiskStoreName(), queue.getMaximumQueueMemory(), listener.getClass().getName(), listenerProperties);
      }

      CliFunctionResult result = new CliFunctionResult(memberId, asyncEventQueueDetails);
      context.getResultSender().lastResult(result);

    } catch (CacheClosedException cce) {
      CliFunctionResult result = new CliFunctionResult(memberId, false, null);
      context.getResultSender().lastResult(result);

    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
      
    } catch (Throwable th) {
      SystemFailure.checkFailure();
      logger.error("Could not list async event queues: {}", th.getMessage(), th);
      CliFunctionResult result = new CliFunctionResult(memberId, th, null);
      context.getResultSender().lastResult(result);
    }
  }
}
