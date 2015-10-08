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

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.ResultSender;
import com.gemstone.gemfire.cache.wan.GatewayReceiver;
import com.gemstone.gemfire.cache.wan.GatewayReceiverFactory;
import com.gemstone.gemfire.cache.wan.GatewayTransportFilter;
import com.gemstone.gemfire.internal.ClassPathLoader;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXml;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.management.internal.cli.CliUtil;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.configuration.domain.XmlEntity;

/**
 * The function to a create GatewayReceiver using given configuration parameters. 
 */
public class GatewayReceiverCreateFunction extends FunctionAdapter implements
    InternalEntity {

  private static final Logger logger = LogService.getLogger();
  
  private static final long serialVersionUID = 8746830191680509335L;

  private static final String ID = GatewayReceiverCreateFunction.class
      .getName();

  public static GatewayReceiverCreateFunction INSTANCE = new GatewayReceiverCreateFunction();

  @Override
  public void execute(FunctionContext context) {
    ResultSender<Object> resultSender = context.getResultSender();

    Cache cache = CacheFactory.getAnyInstance();
    String memberNameOrId = CliUtil.getMemberNameOrId(cache
        .getDistributedSystem().getDistributedMember());

    GatewayReceiverFunctionArgs gatewayReceiverCreateArgs = (GatewayReceiverFunctionArgs)context
        .getArguments();

    try {
      GatewayReceiver createdGatewayReceiver = createGatewayReceiver(cache,
          gatewayReceiverCreateArgs);
      
      Map<String, String> attributes = new HashMap<String, String>();
      if (gatewayReceiverCreateArgs.getStartPort() != null) {
        attributes.put("start-port", gatewayReceiverCreateArgs.getStartPort().toString());
      }
      if (gatewayReceiverCreateArgs.getEndPort() != null) {
        attributes.put("end-port", gatewayReceiverCreateArgs.getEndPort().toString());
      }
      if (gatewayReceiverCreateArgs.getBindAddress() != null) {
        attributes.put("bind-address", gatewayReceiverCreateArgs.getBindAddress());
      }
      XmlEntity xmlEntity = XmlEntity.builder().withType(CacheXml.GATEWAY_RECEIVER).withAttributes(attributes).build();
      resultSender.lastResult(new CliFunctionResult(memberNameOrId, xmlEntity, CliStrings
          .format(
              CliStrings.CREATE_GATEWAYRECEIVER__MSG__GATEWAYRECEIVER_CREATED_ON_0_ONPORT_1,
              new Object[] { memberNameOrId, createdGatewayReceiver.getPort() })));
      
      
    }
    catch (IllegalStateException e) {
      resultSender.lastResult(handleException(memberNameOrId, e.getMessage(), e));
    }
    catch (Exception e) {
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
   * GatewayReceiver creation happens here.
   * 
   * @param     cache
   * @param     gatewayReceiverCreateArgs
   * @return    GatewayReceiver
   */
  private static GatewayReceiver createGatewayReceiver(Cache cache,
      GatewayReceiverFunctionArgs gatewayReceiverCreateArgs) {

    GatewayReceiverFactory gatewayReceiverFactory = cache
        .createGatewayReceiverFactory();

    Integer startPort = gatewayReceiverCreateArgs.getStartPort();
    if (startPort != null) {
      gatewayReceiverFactory.setStartPort(startPort);
    }

    Integer endPort = gatewayReceiverCreateArgs.getEndPort();
    if (endPort != null) {
      gatewayReceiverFactory.setEndPort(endPort);
    }

    String bindAddress = gatewayReceiverCreateArgs.getBindAddress();
    if (bindAddress != null) {
      gatewayReceiverFactory.setBindAddress(bindAddress);
    }

    Integer maxTimeBetweenPings = gatewayReceiverCreateArgs
        .getMaximumTimeBetweenPings();
    if (maxTimeBetweenPings != null) {
      gatewayReceiverFactory.setMaximumTimeBetweenPings(maxTimeBetweenPings);
    }

    Integer socketBufferSize = gatewayReceiverCreateArgs.getSocketBufferSize();
    if (socketBufferSize != null) {
      gatewayReceiverFactory.setSocketBufferSize(socketBufferSize);
    }

    Boolean manualStart = gatewayReceiverCreateArgs.isManualStart();
    if (manualStart != null) {
      gatewayReceiverFactory.setManualStart(manualStart);
    }
    
    String[] gatewayTransportFilters = gatewayReceiverCreateArgs
        .getGatewayTransportFilters();
    if (gatewayTransportFilters != null) {
      for (String gatewayTransportFilter : gatewayTransportFilters) {
        Class gatewayTransportFilterKlass = forName(gatewayTransportFilter,
            CliStrings.CREATE_GATEWAYRECEIVER__GATEWAYTRANSPORTFILTER);
        gatewayReceiverFactory
            .addGatewayTransportFilter((GatewayTransportFilter)newInstance(
                gatewayTransportFilterKlass,
                CliStrings.CREATE_GATEWAYRECEIVER__GATEWAYTRANSPORTFILTER));
      }
    }
    return gatewayReceiverFactory.create();
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
    }
    catch (ClassNotFoundException e) {
      throw new RuntimeException(CliStrings.format(
          CliStrings.CREATE_REGION__MSG__COULDNOT_FIND_CLASS_0_SPECIFIED_FOR_1,
          new Object[] { classToLoadName, neededFor }), e);
    }
    catch (ClassCastException e) {
      throw new RuntimeException(
          CliStrings
              .format(
                  CliStrings.CREATE_REGION__MSG__CLASS_SPECIFIED_FOR_0_SPECIFIED_FOR_1_IS_NOT_OF_EXPECTED_TYPE,
                  new Object[] { classToLoadName, neededFor }), e);
    }

    return loadedClass;
  }

  private static Object newInstance(Class klass, String neededFor) {
    Object instance = null;
    try {
      instance = klass.newInstance();
    }
    catch (InstantiationException e) {
      throw new RuntimeException(
          CliStrings
              .format(
                  CliStrings.CREATE_GATEWAYSENDER__MSG__COULDNOT_INSTANTIATE_CLASS_0_SPECIFIED_FOR_1,
                  new Object[] { klass, neededFor }), e);
    }
    catch (IllegalAccessException e) {
      throw new RuntimeException(
          CliStrings
              .format(
                  CliStrings.CREATE_GATEWAYSENDER__MSG__COULDNOT_ACCESS_CLASS_0_SPECIFIED_FOR_1,
                  new Object[] { klass, neededFor }), e);
    }
    return instance;
  }

  @Override
  public String getId() {
    return ID;
  }

}
