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
package com.gemstone.gemfire.internal.net;

import org.springframework.util.StringUtils;

import com.gemstone.gemfire.GemFireConfigException;
import com.gemstone.gemfire.distributed.SSLEnabledComponents;

public enum SSLEnabledComponent {
  ALL(SSLEnabledComponents.ALL),
  CLUSTER(SSLEnabledComponents.CLUSTER),
  SERVER(SSLEnabledComponents.SERVER),
  JMX(SSLEnabledComponents.JMX),
  HTTP_SERVICE(SSLEnabledComponents.HTTP_SERVICE),
  GATEWAY(SSLEnabledComponents.GATEWAY),
  NONE("NO_COMPONENT");

  private String constant;

  SSLEnabledComponent(final String constant) {
    this.constant = constant;
  }

  public static SSLEnabledComponent getEnum(String enumString) {
    for (SSLEnabledComponent sslEnabledComponent : SSLEnabledComponent.values()) {
      if (!StringUtils.isEmpty(enumString)) {
        if (sslEnabledComponent.constant.equals(enumString)) {
          return sslEnabledComponent;
        }
      }
    }
    throw new GemFireConfigException("There is no registered component for the name: " + enumString);
  }

  public String getConstant() {
    return constant;
  }

  @Override
  public String toString() {
    return getConstant();
  }
}
