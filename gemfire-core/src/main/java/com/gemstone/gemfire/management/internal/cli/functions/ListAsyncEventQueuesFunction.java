/*
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 */
package com.gemstone.gemfire.management.internal.cli.functions;

import java.util.Properties;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventListener;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueue;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.internal.cache.xmlcache.Declarable2;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.management.internal.cli.domain.AsyncEventQueueDetails;

/**
 * An implementation of GemFire Function interface used to determine all the
 * async event queues that exist for the entire cache, distributed across the
 * GemFire distributed system. </p>
 * 
 * @author David Hoots
 * @since 8.0
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
