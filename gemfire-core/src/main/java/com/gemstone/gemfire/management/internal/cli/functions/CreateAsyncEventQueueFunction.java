/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.functions;

/**
 * Function used by the 'create async-event-queue' gfsh command to create an
 * asynchronous event queue on a member.
 * 
 * @author David Hoots
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
      final Object[] args = (Object[]) context.getArguments();
      final String asyncEventQueueId = (String) args[0];
      final boolean isParallel = (Boolean) args[1];
      final boolean enableBatchConflation = (Boolean) args[2];
      final int batchSize = (Integer) args[3];
      final int batchTimeInterval =(Integer) args[4];
      final boolean persistent = (Boolean) args[5];
      final String diskStoreName = (String) args[6];
      final boolean diskSynchronous =(Boolean) args[7];
      final int maxQueueMemory = (Integer) args[8];
      final int dispatcherThreads =(Integer) args[9]; 
      final String orderPolicy= (String) args[10];
      final String[] gatewayEventFilters =(String[]) args[11];
      final String gatewaySubstitutionFilter = (String) args[12];
      final String listenerClassName = (String) args[13];
      final Properties listenerProperties = (Properties) args[14];

      GemFireCacheImpl cache = (GemFireCacheImpl) CacheFactory.getAnyInstance();

      DistributedMember member = cache.getDistributedSystem().getDistributedMember();

      memberId = member.getId();
      // If they set a name use it instead
      if (!member.getName().equals("")) {
        memberId = member.getName();
      }

      AsyncEventQueueFactory asyncEventQueueFactory = cache.createAsyncEventQueueFactory();
      asyncEventQueueFactory.setParallel(isParallel);
      asyncEventQueueFactory.setBatchConflationEnabled(enableBatchConflation);
      asyncEventQueueFactory.setBatchSize(batchSize);
      asyncEventQueueFactory.setBatchTimeInterval(batchTimeInterval);
      asyncEventQueueFactory.setPersistent(persistent);
      asyncEventQueueFactory.setDiskStoreName(diskStoreName);
      asyncEventQueueFactory.setDiskSynchronous(diskSynchronous);
      asyncEventQueueFactory.setMaximumQueueMemory(maxQueueMemory);
      asyncEventQueueFactory.setDispatcherThreads(dispatcherThreads);
      asyncEventQueueFactory.setOrderPolicy(OrderPolicy.valueOf(orderPolicy));
      if (gatewayEventFilters != null) {
        for (String gatewayEventFilter : gatewayEventFilters) {
          Class<?> gatewayEventFilterKlass = forName(gatewayEventFilter, CliStrings.CREATE_ASYNC_EVENT_QUEUE__GATEWAYEVENTFILTER);
          asyncEventQueueFactory.addGatewayEventFilter((GatewayEventFilter) newInstance(gatewayEventFilterKlass, CliStrings.CREATE_ASYNC_EVENT_QUEUE__GATEWAYEVENTFILTER));
        }
      }
      if (gatewaySubstitutionFilter != null) {
        Class<?> gatewayEventSubstitutionFilterKlass = forName(gatewaySubstitutionFilter, CliStrings.CREATE_ASYNC_EVENT_QUEUE__SUBSTITUTION_FILTER);
        asyncEventQueueFactory.setGatewayEventSubstitutionListener((GatewayEventSubstitutionFilter<?,?>) newInstance(gatewayEventSubstitutionFilterKlass, CliStrings.CREATE_ASYNC_EVENT_QUEUE__SUBSTITUTION_FILTER));
      }
      
      Object listenerInstance;
      Class<?> listenerClass = InternalDataSerializer.getCachedClass(listenerClassName);
      listenerInstance = listenerClass.newInstance();

      if (listenerProperties != null && !listenerProperties.isEmpty()) {
        if (!(listenerInstance instanceof Declarable)) {
          throw new IllegalArgumentException("Listener properties were provided, but the listener specified does not implement Declarable.");
        }
        
        ((Declarable) listenerInstance).init(listenerProperties);

        Map<Declarable, Properties> declarablesMap = new HashMap<Declarable, Properties>();
        declarablesMap.put((Declarable) listenerInstance, listenerProperties);
        cache.addDeclarableProperties(declarablesMap);
      }

      asyncEventQueueFactory.create(asyncEventQueueId, (AsyncEventListener) listenerInstance);

      XmlEntity xmlEntity = new XmlEntity(CacheXml.ASYNC_EVENT_QUEUE, "id", asyncEventQueueId);
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
