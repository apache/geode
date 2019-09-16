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

import org.apache.commons.lang3.StringUtils;

import org.apache.geode.cache.configuration.GatewayReceiverConfig;
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
    return receiver;
  }

  private Integer stringToInt(String xmlValue) {
    return StringUtils.isBlank(xmlValue) ? null : Integer.parseInt(xmlValue);
  }

  private String intToString(Integer value) {
    return value == null ? null : value.toString();
  }
}
