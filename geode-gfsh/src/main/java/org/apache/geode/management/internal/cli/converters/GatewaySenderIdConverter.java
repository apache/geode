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

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.internal.ManagementConstants;
import org.apache.geode.management.internal.cli.shell.Gfsh;

/**
 *
 * @since GemFire 7.0
 */
public class GatewaySenderIdConverter extends BaseStringConverter {

  @Override
  public String getConverterHint() {
    return ConverterHint.GATEWAY_SENDER_ID;
  }

  @Override
  public Set<String> getCompletionValues() {
    Set<String> gatewaySenderIds = Collections.emptySet();

    Gfsh gfsh = Gfsh.getCurrentInstance();
    if (gfsh != null && gfsh.isConnectedAndReady()) {
      final String[] gatewaySenderIdArray = (String[]) gfsh.getOperationInvoker().invoke(
          ManagementConstants.OBJECTNAME__DISTRIBUTEDSYSTEM_MXBEAN, "listGatewaySenders",
          new Object[0], new String[0]);
      if (gatewaySenderIdArray != null && gatewaySenderIdArray.length != 0) {
        gatewaySenderIds = new TreeSet<>(Arrays.asList(gatewaySenderIdArray));
      }
    }

    return gatewaySenderIds;
  }

}
