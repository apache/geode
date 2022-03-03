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
package org.apache.geode.management.internal.cli.converters;

import java.util.List;

import org.springframework.shell.core.Completion;
import org.springframework.shell.core.Converter;
import org.springframework.shell.core.MethodTarget;

import org.apache.geode.management.internal.cli.util.ConnectionEndpoint;

public class ConnectionEndpointConverter implements Converter<ConnectionEndpoint> {
  // Defaults
  static final String DEFAULT_JMX_HOST = "localhost";
  static final int DEFAULT_JMX_PORT = 1099;
  static final String DEFAULT_JMX_ENDPOINTS = DEFAULT_JMX_HOST + "[" + DEFAULT_JMX_PORT + "]";

  public static final String DEFAULT_LOCATOR_HOST = "localhost";
  public static final int DEFAULT_LOCATOR_PORT = 10334;
  public static final String DEFAULT_LOCATOR_ENDPOINTS =
      DEFAULT_LOCATOR_HOST + "[" + DEFAULT_LOCATOR_PORT + "]";

  @Override
  public boolean supports(Class<?> type, String optionContext) {
    return ConnectionEndpoint.class == type;
  }

  @Override
  public ConnectionEndpoint convertFromText(String value, Class<?> targetType,
      String optionContext) {
    // expected format host[port], port is optional
    String endpointStr = value.trim();

    String hostStr = DEFAULT_JMX_HOST;
    String portStr = "";
    int port = DEFAULT_JMX_PORT;

    if (!endpointStr.isEmpty()) {
      int openIndex = endpointStr.indexOf("[");
      int closeIndex = endpointStr.indexOf("]");

      if (openIndex != -1) {// might have a port
        if (closeIndex == -1) {
          throw new IllegalArgumentException(
              "Expected input: host[port] or host. Invalid value specified endpoints : " + value);
        }
        hostStr = endpointStr.substring(0, openIndex);

        portStr = endpointStr.substring(openIndex + 1, closeIndex);

        if (portStr.isEmpty()) {
          throw new IllegalArgumentException(
              "Expected input: host[port] or host. Invalid value specified endpoints : " + value);
        }
        try {
          port = Integer.parseInt(portStr);
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException(
              "Expected input: host[port], Port should be a valid number between 1024-65536. Invalid value specified endpoints : "
                  + value);
        }


      } else if (closeIndex != -1) { // shouldn't be there if opening brace was not there
        throw new IllegalArgumentException(
            "Expected input: host[port] or host. Invalid value specified endpoints : " + value);
      } else {// doesn't contain brackets, assume only host name is given & assume default port
        hostStr = endpointStr;
      }
    }

    return new ConnectionEndpoint(hostStr, port);
  }

  @Override
  public boolean getAllPossibleValues(List<Completion> completions, Class<?> targetType,
      String existingData, String optionContext, MethodTarget target) {
    if (ConnectionEndpoint.JMXMANAGER_OPTION_CONTEXT.equals(optionContext)) {
      completions.add(new Completion(DEFAULT_JMX_ENDPOINTS));
    } else if (ConnectionEndpoint.LOCATOR_OPTION_CONTEXT.equals(optionContext)) {
      completions.add(new Completion(DEFAULT_LOCATOR_ENDPOINTS));
    }

    return completions.size() > 0;
  }
}
