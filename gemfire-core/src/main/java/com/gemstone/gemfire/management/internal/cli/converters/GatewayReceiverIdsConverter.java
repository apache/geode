/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
