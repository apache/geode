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

package org.apache.geode.management.internal.configuration.converters;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;

import org.apache.geode.cache.configuration.DeclarableType;
import org.apache.geode.cache.configuration.GatewayReceiverConfig;
import org.apache.geode.cache.configuration.ParameterType;
import org.apache.geode.management.configuration.ClassName;
import org.apache.geode.management.configuration.GatewayReceiver;

public class GatewayReceiverConverter
    extends ConfigurationConverter<GatewayReceiver, GatewayReceiverConfig> {
  @Override
  protected GatewayReceiver fromNonNullXmlObject(GatewayReceiverConfig xmlObject) {
    GatewayReceiver receiver = new GatewayReceiver();
    receiver.setEndPort(stringToInt(xmlObject.getEndPort()));
    receiver.setManualStart(xmlObject.isManualStart());
    receiver.setMaximumTimeBetweenPings(stringToInt(xmlObject.getMaximumTimeBetweenPings()));
    receiver.setSocketBufferSize(stringToInt(xmlObject.getSocketBufferSize()));
    receiver.setStartPort(stringToInt(xmlObject.getStartPort()));
    List<ClassName> configFilters = new ArrayList<>();
    for (DeclarableType xmlFilter : xmlObject.getGatewayTransportFilters()) {
      String className = xmlFilter.getClassName();
      Properties properties = new Properties();
      for (ParameterType parameter : xmlFilter.getParameters()) {
        String parameterValue;
        if (parameter.getString() != null) {
          parameterValue = parameter.getString();
        } else if (parameter.getDeclarable() != null) {
          parameterValue = parameter.getDeclarable().toString();
        } else {
          parameterValue = "";
        }
        properties.setProperty(parameter.getName(), parameterValue);
      }
      configFilters.add(new ClassName(className, properties));
    }
    if (!configFilters.isEmpty()) {
      receiver.setGatewayTransportFilters(configFilters);
    }
    return receiver;
  }

  @Override
  protected GatewayReceiverConfig fromNonNullConfigObject(GatewayReceiver configObject) {
    GatewayReceiverConfig receiver = new GatewayReceiverConfig();
    receiver.setEndPort(intToString(configObject.getEndPort()));
    receiver.setStartPort(intToString(configObject.getStartPort()));
    receiver.setManualStart(configObject.isManualStart());
    receiver.setMaximumTimeBetweenPings(intToString(configObject.getMaximumTimeBetweenPings()));
    receiver.setSocketBufferSize(intToString(configObject.getSocketBufferSize()));
    if (configObject.getGatewayTransportFilters() != null) {
      List<DeclarableType> xmlFilters = receiver.getGatewayTransportFilters();
      for (ClassName configFilter : configObject.getGatewayTransportFilters()) {
        String className = configFilter.getClassName();
        Properties props = configFilter.getInitProperties();
        xmlFilters.add(new DeclarableType(className, props));
      }
    }
    return receiver;
  }

  private Integer stringToInt(String xmlValue) {
    return StringUtils.isBlank(xmlValue) ? null : Integer.parseInt(xmlValue);
  }

  private String intToString(Integer value) {
    return value == null ? null : value.toString();
  }
}
