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

import org.apache.geode.cache.configuration.GatewayReceiverConfig;
import org.apache.geode.management.configuration.GatewayReceiver;

public class GatewayReceiverConverter
    extends ConfigurationConverter<GatewayReceiver, GatewayReceiverConfig> {
  @Override
  protected GatewayReceiver fromNonNullXmlObject(GatewayReceiverConfig xmlObject) {
    GatewayReceiver receiver = new GatewayReceiver();
    receiver.setEndPort(xmlObject.getEndPort());
    if (xmlObject.isManualStart() != null) {
      receiver.setManualStart(xmlObject.isManualStart());
    }
    receiver.setMaximumTimeBetweenPings(xmlObject.getMaximumTimeBetweenPings());
    receiver.setSocketBufferSize(xmlObject.getSocketBufferSize());
    receiver.setStartPort(xmlObject.getStartPort());
    return receiver;
  }

  @Override
  protected GatewayReceiverConfig fromNonNullConfigObject(GatewayReceiver configObject) {
    GatewayReceiverConfig receiver = new GatewayReceiverConfig();
    receiver.setEndPort(configObject.getEndPort());
    receiver.setStartPort(configObject.getStartPort());
    receiver.setManualStart(configObject.isManualStart());
    receiver.setMaximumTimeBetweenPings(configObject.getMaximumTimeBetweenPings());
    receiver.setSocketBufferSize(configObject.getSocketBufferSize());
    return receiver;
  }
}
