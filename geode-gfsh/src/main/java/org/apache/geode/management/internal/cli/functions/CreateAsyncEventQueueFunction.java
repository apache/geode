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
import java.util.List;
import java.util.Map;
import java.util.Properties;

import joptsimple.internal.Strings;
import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.asyncqueue.AsyncEventQueueFactory;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.ClassNameType;
import org.apache.geode.cache.configuration.DeclarableType;
import org.apache.geode.cache.configuration.ParameterType;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.wan.GatewayEventFilter;
import org.apache.geode.cache.wan.GatewayEventSubstitutionFilter;
import org.apache.geode.cache.wan.GatewaySender.OrderPolicy;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.cli.CliFunction;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.functions.CliFunctionResult.StatusState;

/**
 * Function used by the 'create async-event-queue' gfsh command to create an asynchronous event
 * queue on a member.
 *
 * @since GemFire 8.0
 */
public class CreateAsyncEventQueueFunction extends CliFunction<CacheConfig.AsyncEventQueue> {
  private static final Logger logger = LogService.getLogger();

  private static final long serialVersionUID = 1L;

  @Override
  public CliFunctionResult executeFunction(FunctionContext<CacheConfig.AsyncEventQueue> context) {
    // Declared here so that it's available when returning a Throwable
    String memberId = "";

    try {
      CacheConfig.AsyncEventQueue config = context.getArguments();

      InternalCache cache = (InternalCache) context.getCache();

      DistributedMember member = cache.getDistributedSystem().getDistributedMember();

      memberId = member.getId();
      // If they set a name use it instead
      if (!member.getName().equals("")) {
        memberId = member.getName();
      }

      AsyncEventQueueFactory asyncEventQueueFactory =
          cache.createAsyncEventQueueFactory().setParallel(config.isParallel())
              .setBatchConflationEnabled(config.isEnableBatchConflation())
              .setBatchSize(Integer.parseInt(config.getBatchSize()))
              .setBatchTimeInterval(Integer.parseInt(config.getBatchTimeInterval()))
              .setPersistent(config.isPersistent()).setDiskStoreName(config.getDiskStoreName())
              .setDiskSynchronous(config.isDiskSynchronous())
              .setForwardExpirationDestroy(config.isForwardExpirationDestroy())
              .setMaximumQueueMemory(Integer.parseInt(config.getMaximumQueueMemory()))
              .setDispatcherThreads(Integer.parseInt(config.getDispatcherThreads()))
              .setOrderPolicy(OrderPolicy.valueOf(config.getOrderPolicy()));

      if (config.isPauseEventProcessing()) {
        asyncEventQueueFactory.pauseEventDispatching();
      }

      String[] gatewayEventFilters = config.getGatewayEventFilters().stream()
          .map(ClassNameType::getClassName).toArray(String[]::new);

      for (String gatewayEventFilter : gatewayEventFilters) {
        asyncEventQueueFactory
            .addGatewayEventFilter((GatewayEventFilter) newInstance(gatewayEventFilter));
      }

      DeclarableType gatewayEventSubstitutionFilter = config.getGatewayEventSubstitutionFilter();

      if (gatewayEventSubstitutionFilter != null) {
        String gatewaySubstitutionFilter = gatewayEventSubstitutionFilter.getClassName();
        asyncEventQueueFactory.setGatewayEventSubstitutionListener(
            (GatewayEventSubstitutionFilter<?, ?>) newInstance(gatewaySubstitutionFilter));
      }

      String listenerClassName = config.getAsyncEventListener().getClassName();
      Object listenerInstance;
      Class<?> listenerClass = InternalDataSerializer.getCachedClass(listenerClassName);
      listenerInstance = listenerClass.newInstance();

      List<ParameterType> parameters = config.getAsyncEventListener().getParameters();
      Properties listenerProperties = new Properties();
      for (ParameterType p : parameters) {
        listenerProperties.put(p.getName(), p.getString());
      }

      if (!listenerProperties.isEmpty()) {
        if (!(listenerInstance instanceof Declarable)) {
          throw new IllegalArgumentException(
              "Listener properties were provided, but the listener specified does not implement Declarable.");
        }

        ((Declarable) listenerInstance).initialize(cache, listenerProperties);
        legacyInit((Declarable) listenerInstance, listenerProperties);

        Map<Declarable, Properties> declarablesMap = new HashMap<>();
        declarablesMap.put((Declarable) listenerInstance, listenerProperties);
        cache.addDeclarableProperties(declarablesMap);
      }

      asyncEventQueueFactory.create(config.getId(), (AsyncEventListener) listenerInstance);

      return new CliFunctionResult(memberId, StatusState.OK, "Success");
    } catch (CacheClosedException cce) {
      return new CliFunctionResult(memberId, StatusState.ERROR, null);
    } catch (Exception e) {
      logger.error("Could not create async event queue: {}", e.getMessage(), e);
      return new CliFunctionResult(memberId, e, null);
    }
  }

  @SuppressWarnings("deprecation")
  private void legacyInit(Declarable listenerInstance, Properties listenerProperties) {
    listenerInstance.init(listenerProperties); // for backwards compatibility
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
    return CreateAsyncEventQueueFunction.class.getName();
  }
}
