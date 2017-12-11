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

import joptsimple.internal.Strings;
import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.cache.wan.GatewayReceiverFactory;
import org.apache.geode.cache.wan.GatewayTransportFilter;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.internal.cache.xmlcache.CacheXml;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;

/**
 * The function to a create GatewayReceiver using given configuration parameters.
 */
public class GatewayReceiverCreateFunction implements Function, InternalEntity {

  private static final Logger logger = LogService.getLogger();

  private static final long serialVersionUID = 8746830191680509335L;

  private static final String ID = GatewayReceiverCreateFunction.class.getName();

  public static GatewayReceiverCreateFunction INSTANCE = new GatewayReceiverCreateFunction();

  @Override
  public void execute(FunctionContext context) {
    ResultSender<Object> resultSender = context.getResultSender();

    Cache cache = context.getCache();
    String memberNameOrId = context.getMemberName();

    GatewayReceiverFunctionArgs gatewayReceiverCreateArgs =
        (GatewayReceiverFunctionArgs) context.getArguments();

    try {
      GatewayReceiver createdGatewayReceiver =
          createGatewayReceiver(cache, gatewayReceiverCreateArgs);

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
      XmlEntity xmlEntity = XmlEntity.builder().withType(CacheXml.GATEWAY_RECEIVER)
          .withAttributes(attributes).build();
      resultSender.lastResult(new CliFunctionResult(memberNameOrId, xmlEntity,
          CliStrings.format(
              CliStrings.CREATE_GATEWAYRECEIVER__MSG__GATEWAYRECEIVER_CREATED_ON_0_ONPORT_1,
              new Object[] {memberNameOrId, createdGatewayReceiver.getPort()})));
    } catch (IllegalStateException e) {
      // no need to log the stack trace
      resultSender.lastResult(new CliFunctionResult(memberNameOrId, e, e.getMessage()));
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      resultSender.lastResult(new CliFunctionResult(memberNameOrId, e, e.getMessage()));
    }

  }

  /**
   * GatewayReceiver creation happens here.
   *
   * @param cache
   * @param gatewayReceiverCreateArgs
   * @return GatewayReceiver
   */
  GatewayReceiver createGatewayReceiver(Cache cache,
      GatewayReceiverFunctionArgs gatewayReceiverCreateArgs)
      throws IllegalAccessException, InstantiationException, ClassNotFoundException {

    GatewayReceiverFactory gatewayReceiverFactory = cache.createGatewayReceiverFactory();

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

    Integer maxTimeBetweenPings = gatewayReceiverCreateArgs.getMaximumTimeBetweenPings();
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

    String[] gatewayTransportFilters = gatewayReceiverCreateArgs.getGatewayTransportFilters();
    if (gatewayTransportFilters != null) {
      for (String gatewayTransportFilter : gatewayTransportFilters) {
        gatewayReceiverFactory.addGatewayTransportFilter(
            (GatewayTransportFilter) newInstance(gatewayTransportFilter));
      }
    }

    String hostnameForSenders = gatewayReceiverCreateArgs.getHostnameForSenders();
    if (hostnameForSenders != null) {
      gatewayReceiverFactory.setHostnameForSenders(hostnameForSenders);
    }
    return gatewayReceiverFactory.create();
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
    return ID;
  }

}
