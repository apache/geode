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
package com.gemstone.gemfire.management.internal.cli.converters;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import com.gemstone.gemfire.management.cli.ConverterHint;
import com.gemstone.gemfire.management.internal.ManagementConstants;
import com.gemstone.gemfire.management.internal.cli.shell.Gfsh;

import org.springframework.shell.core.Completion;
import org.springframework.shell.core.Converter;
import org.springframework.shell.core.MethodTarget;

public class GatewayReceiverIdsConverter implements Converter<String> {
  @Override
  public boolean supports(Class<?> type, String optionContext) {
    return false;
  }

  @Override
  public String convertFromText(String value, Class<?> targetType,
      String optionContext) {
    return value;
  }

  @Override
  public boolean getAllPossibleValues(List<Completion> completions,
      Class<?> targetType, String existingData, String optionContext,
      MethodTarget target) {
    if (String.class.equals(targetType) && ConverterHint.GATEWAY_RECEIVER_ID.equals(optionContext)) {
      Set<String> gatewaySenderIds = getGatewayRecieverIds();

      for (String gatewaySenderId : gatewaySenderIds) {
        completions.add(new Completion(gatewaySenderId));
      }
    }

    return !completions.isEmpty();
  }

  public Set<String> getGatewayRecieverIds() {
    Set<String> gatewayRecieverIds = new HashSet<String>();

    Gfsh gfsh = Gfsh.getCurrentInstance();
    if (gfsh != null && gfsh.isConnectedAndReady()) {
      final String[] gatewaySenderIdArray = (String[]) gfsh.getOperationInvoker().invoke(
        ManagementConstants.OBJECTNAME__DISTRIBUTEDSYSTEM_MXBEAN, "listGatewayReceivers", new Object[0], new String[0]);
      gatewayRecieverIds = new TreeSet<String>(Arrays.asList(gatewaySenderIdArray));
    }

    return gatewayRecieverIds;
  }

}
