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
package org.apache.geode.internal.security;

import java.util.Arrays;

import org.apache.geode.GemFireConfigException;
import org.apache.geode.security.SecurableCommunicationChannels;

public enum SecurableCommunicationChannel {
  ALL(SecurableCommunicationChannels.ALL),
  CLUSTER(SecurableCommunicationChannels.CLUSTER),
  SERVER(SecurableCommunicationChannels.SERVER),
  JMX(SecurableCommunicationChannels.JMX),
  WEB(SecurableCommunicationChannels.WEB),
  GATEWAY(SecurableCommunicationChannels.GATEWAY),
  LOCATOR(SecurableCommunicationChannels.LOCATOR);

  private final String constant;

  SecurableCommunicationChannel(final String constant) {
    this.constant = constant;
  }

  public String getConstant() {
    return constant;
  }

  @Override
  public String toString() {
    return constant;
  }

  public static SecurableCommunicationChannel getEnum(String enumString) {
    return Arrays.stream(values()).filter(ch -> ch.constant.equalsIgnoreCase(enumString)).findAny()
        .orElseThrow(() -> new GemFireConfigException(
            "There is no registered component for the name: " + enumString));
  }
}
