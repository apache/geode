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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import joptsimple.internal.Strings;
import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.asyncqueue.AsyncEventQueueFactory;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.wan.GatewayEventFilter;
import org.apache.geode.cache.wan.GatewayEventSubstitutionFilter;
import org.apache.geode.cache.wan.GatewaySender.OrderPolicy;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.xmlcache.CacheXml;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;

/**
 * Function used by the 'create async-event-queue' gfsh command to create an asynchronous event
 * queue on a member.
 *
 * @since GemFire 8.0
 */
public class CreateAsyncEventQueueFunction extends FunctionAdapter implements InternalEntity {
  private static final Logger logger = LogService.getLogger();

  private static final long serialVersionUID = 1L;

  @SuppressWarnings("deprecation")
  @Override
  public void execute(FunctionContext context) {
    // Declared here so that it's available when returning a Throwable
    String memberId = "";

    try {
      AsyncEventQueueFunctionArgs aeqArgs = (AsyncEventQueueFunctionArgs) context.getArguments();

      InternalCache cache = (InternalCache) context.getCache();

      DistributedMember member = cache.getDistributedSystem().getDistributedMember();

      memberId = member.getId();
      // If they set a name use it instead
      if (!member.getName().equals("")) {
        memberId = member.getName();
      }

      AsyncEventQueueFactory asyncEventQueueFactory = cache.createAsyncEventQueueFactory()
          .setParallel(aeqArgs.isParallel())
          .setBatchConflationEnabled(aeqArgs.isEnableBatchConflation())
          .setBatchSize(aeqArgs.getBatchSize()).setBatchTimeInterval(aeqArgs.getBatchTimeInterval())
          .setPersistent(aeqArgs.isPersistent()).setDiskStoreName(aeqArgs.getDiskStoreName())
          .setDiskSynchronous(aeqArgs.isDiskSynchronous())
          .setForwardExpirationDestroy(aeqArgs.isForwardExpirationDestroy())
          .setMaximumQueueMemory(aeqArgs.getMaxQueueMemory())
          .setDispatcherThreads(aeqArgs.getDispatcherThreads())
          .setOrderPolicy(OrderPolicy.valueOf(aeqArgs.getOrderPolicy()));

      String[] gatewayEventFilters = aeqArgs.getGatewayEventFilters();
      if (gatewayEventFilters != null) {
        for (String gatewayEventFilter : gatewayEventFilters) {
          asyncEventQueueFactory
              .addGatewayEventFilter((GatewayEventFilter) newInstance(gatewayEventFilter));
        }
      }

      String gatewaySubstitutionFilter = aeqArgs.getGatewaySubstitutionFilter();
      if (gatewaySubstitutionFilter != null) {
        asyncEventQueueFactory.setGatewayEventSubstitutionListener(
            (GatewayEventSubstitutionFilter<?, ?>) newInstance(gatewaySubstitutionFilter));
      }

      String listenerClassName = aeqArgs.getListenerClassName();
      Object listenerInstance;
      Class<?> listenerClass = InternalDataSerializer.getCachedClass(listenerClassName);
      listenerInstance = listenerClass.newInstance();

      Properties listenerProperties = aeqArgs.getListenerProperties();
      if (listenerProperties != null && !listenerProperties.isEmpty()) {
        if (!(listenerInstance instanceof Declarable)) {
          throw new IllegalArgumentException(
              "Listener properties were provided, but the listener specified does not implement Declarable.");
        }

        ((Declarable) listenerInstance).init(listenerProperties);

        Map<Declarable, Properties> declarablesMap = new HashMap<Declarable, Properties>();
        declarablesMap.put((Declarable) listenerInstance, listenerProperties);
        cache.addDeclarableProperties(declarablesMap);
      }

      asyncEventQueueFactory.create(aeqArgs.getAsyncEventQueueId(),
          (AsyncEventListener) listenerInstance);

      XmlEntity xmlEntity =
          new XmlEntity(CacheXml.ASYNC_EVENT_QUEUE, "id", aeqArgs.getAsyncEventQueueId());
      context.getResultSender().lastResult(new CliFunctionResult(memberId, xmlEntity, "Success"));

    } catch (CacheClosedException cce) {
      context.getResultSender().lastResult(new CliFunctionResult(memberId, false, null));
    } catch (Exception e) {
      logger.error("Could not create async event queue: {}", e.getMessage(), e);
      context.getResultSender().lastResult(new CliFunctionResult(memberId, e, null));
    }
  }

  private Object newInstance(String className)
      throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    if (Strings.isNullOrEmpty(className)) {
      return null;
    }

    return ClassPathLoader.getLatest().forName(className).newInstance();
  }

  @Override
  public String getId() {
    return CreateDiskStoreFunction.class.getName();
  }
}
