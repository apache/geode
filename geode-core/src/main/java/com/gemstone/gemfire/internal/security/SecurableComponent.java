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
package com.gemstone.gemfire.internal.security;

import org.springframework.util.StringUtils;

import com.gemstone.gemfire.GemFireConfigException;
import org.apache.geode.security.SecurableComponents;

public enum SecurableComponent {
  ALL(SecurableComponents.ALL),
  CLUSTER(SecurableComponents.CLUSTER),
  SERVER(SecurableComponents.SERVER),
  JMX(SecurableComponents.JMX),
  HTTP_SERVICE(SecurableComponents.HTTP_SERVICE),
  GATEWAY(SecurableComponents.GATEWAY),
  NONE("NO_COMPONENT");

  private final String constant;

  SecurableComponent(final String constant) {
    this.constant = constant;
  }

  public static SecurableComponent getEnum(String enumString) {
    for (SecurableComponent securableComponent : SecurableComponent.values()) {
      if (!StringUtils.isEmpty(enumString)) {
        if (securableComponent.constant.equals(enumString)) {
          return securableComponent;
        }
      }
    }
    throw new GemFireConfigException("There is no registered component for the name: " + enumString);
  }

  public String getConstant() {
    return constant;
  }
}
