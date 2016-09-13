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

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.wan.GatewaySender.OrderPolicy;
import org.apache.geode.cache.wan.GatewayEventFilter;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.cache.wan.GatewaySenderFactory;
import org.apache.geode.cache.wan.GatewayTransportFilter;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.internal.cache.wan.GatewaySenderException;
import org.apache.geode.internal.cache.xmlcache.CacheXml;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;

public class GatewaySenderCreateFunction extends FunctionAdapter implements
    InternalEntity {

  private static final Logger logger = LogService.getLogger();
  
  private static final long serialVersionUID = 8746830191680509335L;

  private static final String ID = GatewaySenderCreateFunction.class.getName();
  
  public static GatewaySenderCreateFunction INSTANCE = new GatewaySenderCreateFunction();


  @Override
  public void execute(FunctionContext context) {
    ResultSender<Object> resultSender = context.getResultSender();

    Cache  cache          = CacheFactory.getAnyInstance();
    String memberNameOrId = CliUtil.getMemberNameOrId(cache.getDistributedSystem().getDistributedMember());

    GatewaySenderFunctionArgs gatewaySenderCreateArgs = (GatewaySenderFunctionArgs) context.getArguments();
    
    try {
      GatewaySender createdGatewaySender = createGatewaySender(cache, gatewaySenderCreateArgs);
      XmlEntity xmlEntity = new XmlEntity(CacheXml.GATEWAY_SENDER, "id", gatewaySenderCreateArgs.getId());
      resultSender.lastResult(new CliFunctionResult(memberNameOrId, xmlEntity, CliStrings.format(CliStrings.CREATE_GATEWAYSENDER__MSG__GATEWAYSENDER_0_CREATED_ON_1, new Object[] {createdGatewaySender.getId(), memberNameOrId})));
    } catch (GatewaySenderException e) {
      resultSender.lastResult(handleException(memberNameOrId, e.getMessage(), e));
    } catch (Exception e) {
      String exceptionMsg = e.getMessage();
      if (exceptionMsg == null) {
        exceptionMsg = CliUtil.stackTraceAsString(e);
      }
      resultSender.lastResult(handleException(memberNameOrId, exceptionMsg, e));
    }
  }
  
  private CliFunctionResult handleException(final String memberNameOrId,
      final String exceptionMsg, final Exception e) {
    if (e != null && logger.isDebugEnabled()) {
      logger.debug(e.getMessage(), e);
    }
    if (exceptionMsg != null) {
      return new CliFunctionResult(memberNameOrId, false, exceptionMsg);
    }
    
    return new CliFunctionResult(memberNameOrId);
  }
  
  /**
   * Creates the GatewaySender with given configuration.
   * 
   * @param     cache
   * @param     gatewaySenderCreateArgs
   * @return    GatewaySender
   */
  private static GatewaySender createGatewaySender(Cache cache, GatewaySenderFunctionArgs gatewaySenderCreateArgs) {
    GatewaySenderFactory gateway = cache.createGatewaySenderFactory();
    
    Boolean isParallel = gatewaySenderCreateArgs.isParallel();
    if (isParallel != null) {
      gateway.setParallel(isParallel);
    }
    
    Boolean manualStart = gatewaySenderCreateArgs.isManualStart();
    if (manualStart != null) {
      gateway.setManualStart(manualStart);
    }
    
    Integer maxQueueMemory = gatewaySenderCreateArgs.getMaxQueueMemory();
    if (maxQueueMemory != null) {
      gateway.setMaximumQueueMemory(maxQueueMemory);
    }
    
    Integer batchSize = gatewaySenderCreateArgs.getBatchSize();
    if (batchSize != null) {
      gateway.setBatchSize(batchSize);
    }
    
    Integer batchTimeInterval = gatewaySenderCreateArgs.getBatchTimeInterval();
    if (batchTimeInterval != null) {
      gateway.setBatchTimeInterval(batchTimeInterval);
    }
    
    Boolean enableBatchConflation = gatewaySenderCreateArgs.isBatchConflationEnabled();
    if (enableBatchConflation != null) {
      gateway.setBatchConflationEnabled(enableBatchConflation);
    }
    
    Integer socketBufferSize = gatewaySenderCreateArgs.getSocketBufferSize();
    if (socketBufferSize != null) {
      gateway.setSocketBufferSize(socketBufferSize);
    }
    
    Integer socketReadTimeout = gatewaySenderCreateArgs.getSocketReadTimeout();
    if (socketReadTimeout != null) {
      gateway.setSocketReadTimeout(socketReadTimeout);
    }
    
    Integer alertThreshold = gatewaySenderCreateArgs.getAlertThreshold();
    if (alertThreshold != null) {
      gateway.setAlertThreshold(alertThreshold);
    }
    
    Integer dispatcherThreads = gatewaySenderCreateArgs.getDispatcherThreads();
    if (dispatcherThreads != null && dispatcherThreads > 1) {
      gateway.setDispatcherThreads(dispatcherThreads);
      
      String orderPolicy = gatewaySenderCreateArgs.getOrderPolicy();
      gateway.setOrderPolicy(OrderPolicy.valueOf(orderPolicy));
    }
    
    Boolean isPersistenceEnabled = gatewaySenderCreateArgs.isPersistenceEnabled();
    if (isPersistenceEnabled != null) {
      gateway.setPersistenceEnabled(isPersistenceEnabled);
    }

    String diskStoreName = gatewaySenderCreateArgs.getDiskStoreName();
    if (diskStoreName != null) {
      gateway.setDiskStoreName(diskStoreName);
    }
    
    Boolean isDiskSynchronous = gatewaySenderCreateArgs.isDiskSynchronous();
    if (isDiskSynchronous != null) {
      gateway.setDiskSynchronous(isDiskSynchronous);
    }
    
    String[] gatewayEventFilters = gatewaySenderCreateArgs.getGatewayEventFilter();
    if (gatewayEventFilters != null) {
      for (String gatewayEventFilter : gatewayEventFilters) {
        Class gatewayEventFilterKlass = forName(gatewayEventFilter, CliStrings.CREATE_GATEWAYSENDER__GATEWAYEVENTFILTER);
        gateway.addGatewayEventFilter((GatewayEventFilter) newInstance(gatewayEventFilterKlass, CliStrings.CREATE_GATEWAYSENDER__GATEWAYEVENTFILTER));
      }
    }
    
    String[] gatewayTransportFilters = gatewaySenderCreateArgs.getGatewayTransportFilter();
    if (gatewayTransportFilters != null) {
      for (String gatewayTransportFilter : gatewayTransportFilters) {
        Class gatewayTransportFilterKlass = forName(gatewayTransportFilter, CliStrings.CREATE_GATEWAYSENDER__GATEWAYTRANSPORTFILTER);
        gateway.addGatewayTransportFilter((GatewayTransportFilter) newInstance(gatewayTransportFilterKlass, CliStrings.CREATE_GATEWAYSENDER__GATEWAYTRANSPORTFILTER));
      }
    }
    return gateway.create(gatewaySenderCreateArgs.getId(), gatewaySenderCreateArgs.getRemoteDistributedSystemId());
  }

  @SuppressWarnings("unchecked")
  private static Class forName(String classToLoadName, String neededFor) {
    Class loadedClass = null;
    try {
      // Set Constraints
      ClassPathLoader classPathLoader = ClassPathLoader.getLatest();
      if (classToLoadName != null && !classToLoadName.isEmpty()) {
        loadedClass = classPathLoader.forName(classToLoadName);
      }
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(CliStrings.format(CliStrings.CREATE_REGION__MSG__COULDNOT_FIND_CLASS_0_SPECIFIED_FOR_1, new Object[] {classToLoadName, neededFor}), e);
    } catch (ClassCastException e) {
      throw new RuntimeException(CliStrings.format(CliStrings.CREATE_REGION__MSG__CLASS_SPECIFIED_FOR_0_SPECIFIED_FOR_1_IS_NOT_OF_EXPECTED_TYPE, new Object[] {classToLoadName, neededFor}), e);
    }

    return loadedClass;
  }
  
  private static Object newInstance(Class klass, String neededFor) {
    Object instance = null;
    try {
      instance = klass.newInstance();
    } catch (InstantiationException e) {
      throw new RuntimeException(CliStrings.format(CliStrings.CREATE_GATEWAYSENDER__MSG__COULDNOT_INSTANTIATE_CLASS_0_SPECIFIED_FOR_1, new Object[] {klass, neededFor}), e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(CliStrings.format(CliStrings.CREATE_GATEWAYSENDER__MSG__COULDNOT_ACCESS_CLASS_0_SPECIFIED_FOR_1, new Object[] {klass, neededFor}), e);
    }
    return instance;
  }
  
  @Override
  public String getId() {
    return ID;
  }

}
