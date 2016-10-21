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

import org.apache.geode.security.SecurableCommunicationChannels;

import org.apache.geode.GemFireConfigException;

public enum SecurableCommunicationChannel {
  ALL(SecurableCommunicationChannels.ALL), CLUSTER(SecurableCommunicationChannels.CLUSTER), SERVER(
      SecurableCommunicationChannels.SERVER), JMX(SecurableCommunicationChannels.JMX), WEB(
          SecurableCommunicationChannels.WEB), GATEWAY(
              SecurableCommunicationChannels.GATEWAY), LOCATOR(
                  SecurableCommunicationChannels.LOCATOR), NONE("NO_COMPONENT");

  private final String constant;

  SecurableCommunicationChannel(final String constant) {
    this.constant = constant;
  }

  public static SecurableCommunicationChannel getEnum(String enumString) {
    for (SecurableCommunicationChannel securableCommunicationChannel : SecurableCommunicationChannel
        .values()) {
      if (securableCommunicationChannel.constant.equalsIgnoreCase(enumString)) {
        return securableCommunicationChannel;
      }
    }
    throw new GemFireConfigException(
        "There is no registered component for the name: " + enumString);
  }

  public String getConstant() {
    return constant;
  }

  @Override
  public String toString() {
    return constant;
  }
}
