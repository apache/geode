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
package com.gemstone.gemfire.management.internal.cli.functions;

/**
 * Function used by the 'create async-event-queue' gfsh command to create an
 * asynchronous event queue on a member.
 * 
 * @since 8.0
 */
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventListener;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueueFactory;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.wan.GatewaySender.OrderPolicy;
import com.gemstone.gemfire.cache.wan.GatewayEventFilter;
import com.gemstone.gemfire.cache.wan.GatewayEventSubstitutionFilter;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.ClassPathLoader;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXml;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.configuration.domain.XmlEntity;
import joptsimple.internal.Strings;

public class CreateAsyncEventQueueFunction extends FunctionAdapter implements InternalEntity {
  private static final Logger logger = LogService.getLogger();
  
  private static final long serialVersionUID = 1L;

  @SuppressWarnings("deprecation")
  @Override
  public void execute(FunctionContext context) {
    // Declared here so that it's available when returning a Throwable
    String memberId = "";

    try {
      AsyncEventQueueFunctionArgs aeqArgs =  (AsyncEventQueueFunctionArgs)context.getArguments();
      
      GemFireCacheImpl cache = (GemFireCacheImpl) CacheFactory.getAnyInstance();

      DistributedMember member = cache.getDistributedSystem().getDistributedMember();

      memberId = member.getId();
      // If they set a name use it instead
      if (!member.getName().equals("")) {
        memberId = member.getName();
      }

      AsyncEventQueueFactory asyncEventQueueFactory = cache.createAsyncEventQueueFactory()
          .setParallel(aeqArgs.isParallel())
          .setBatchConflationEnabled(aeqArgs.isEnableBatchConflation())
          .setBatchSize(aeqArgs.getBatchSize())
          .setBatchTimeInterval(aeqArgs.getBatchTimeInterval())
          .setPersistent(aeqArgs.isPersistent())
          .setDiskStoreName(aeqArgs.getDiskStoreName())
          .setDiskSynchronous(aeqArgs.isDiskSynchronous())
          .setIgnoreEvictionAndExpiration(aeqArgs.isIgnoreEvictionAndExpiration())
          .setMaximumQueueMemory(aeqArgs.getMaxQueueMemory())
          .setDispatcherThreads(aeqArgs.getDispatcherThreads())
          .setOrderPolicy(OrderPolicy.valueOf(aeqArgs.getOrderPolicy()));

      String[] gatewayEventFilters = aeqArgs.getGatewayEventFilters();
      if (gatewayEventFilters != null) {
        for (String gatewayEventFilter : gatewayEventFilters) {
          Class<?> gatewayEventFilterKlass = forName(gatewayEventFilter, CliStrings.CREATE_ASYNC_EVENT_QUEUE__GATEWAYEVENTFILTER);
          asyncEventQueueFactory.addGatewayEventFilter((GatewayEventFilter) newInstance(gatewayEventFilterKlass, CliStrings.CREATE_ASYNC_EVENT_QUEUE__GATEWAYEVENTFILTER));
        }
      }
      
      String gatewaySubstitutionFilter = aeqArgs.getGatewaySubstitutionFilter();
      if (gatewaySubstitutionFilter != null) {
        Class<?> gatewayEventSubstitutionFilterKlass = forName(gatewaySubstitutionFilter, CliStrings.CREATE_ASYNC_EVENT_QUEUE__SUBSTITUTION_FILTER);
        asyncEventQueueFactory.setGatewayEventSubstitutionListener((GatewayEventSubstitutionFilter<?,?>) newInstance(gatewayEventSubstitutionFilterKlass, CliStrings.CREATE_ASYNC_EVENT_QUEUE__SUBSTITUTION_FILTER));
      }
    
      String listenerClassName = aeqArgs.getListenerClassName();
      Object listenerInstance;
      Class<?> listenerClass = InternalDataSerializer.getCachedClass(listenerClassName);
      listenerInstance = listenerClass.newInstance();

      Properties listenerProperties = aeqArgs.getListenerProperties();
      if (listenerProperties != null && !listenerProperties.isEmpty()) {
        if (!(listenerInstance instanceof Declarable)) {
          throw new IllegalArgumentException("Listener properties were provided, but the listener specified does not implement Declarable.");
        }
        
        ((Declarable) listenerInstance).init(listenerProperties);

        Map<Declarable, Properties> declarablesMap = new HashMap<Declarable, Properties>();
        declarablesMap.put((Declarable) listenerInstance, listenerProperties);
        cache.addDeclarableProperties(declarablesMap);
      }

      asyncEventQueueFactory.create(aeqArgs.getAsyncEventQueueId(), (AsyncEventListener) listenerInstance);

      XmlEntity xmlEntity = new XmlEntity(CacheXml.ASYNC_EVENT_QUEUE, "id", aeqArgs.getAsyncEventQueueId());
      context.getResultSender().lastResult(new CliFunctionResult(memberId, xmlEntity, "Success"));

    } catch (CacheClosedException cce) {
      context.getResultSender().lastResult(new CliFunctionResult(memberId, false, null));

    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
      
    } catch (Throwable th) {
      SystemFailure.checkFailure();
      logger.error("Could not create async event queue: {}", th.getMessage(), th);
      context.getResultSender().lastResult(new CliFunctionResult(memberId, th, null));
    }
  }

  private Class<?> forName(String className, String neededFor) {
    if (Strings.isNullOrEmpty(className)) {
      return null;
    }
    
    try {
      return ClassPathLoader.getLatest().forName(className);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(CliStrings.format(CliStrings.CREATE_ASYNC_EVENT_QUEUE__MSG__COULDNOT_FIND_CLASS_0_SPECIFIED_FOR_1,
          new Object[] { className, neededFor }), e);
    } catch (ClassCastException e) {
      throw new RuntimeException(CliStrings.format(
          CliStrings.CREATE_ASYNC_EVENT_QUEUE__MSG__CLASS_0_SPECIFIED_FOR_1_IS_NOT_OF_EXPECTED_TYPE, new Object[] { className,
              neededFor }), e);
    }
  }
  
  private static Object newInstance(Class<?> klass, String neededFor) {
    try {
      return klass.newInstance();
    } catch (InstantiationException e) {
      throw new RuntimeException(CliStrings.format(CliStrings.CREATE_ASYNC_EVENT_QUEUE__MSG__COULDNOT_INSTANTIATE_CLASS_0_SPECIFIED_FOR_1, new Object[] { klass,
              neededFor }), e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(CliStrings.format(
          CliStrings.CREATE_ASYNC_EVENT_QUEUE__MSG__COULDNOT_ACCESS_CLASS_0_SPECIFIED_FOR_1, new Object[] { klass, neededFor }), e);
    }
  }
  
  @Override
  public String getId() {
    return CreateDiskStoreFunction.class.getName();
  }
}
