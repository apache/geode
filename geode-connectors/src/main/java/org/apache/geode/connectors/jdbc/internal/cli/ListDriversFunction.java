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
package org.apache.geode.connectors.jdbc.internal.cli;

import java.util.List;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.util.DriverJarUtils;
import org.apache.geode.management.cli.CliFunction;
import org.apache.geode.management.internal.functions.CliFunctionResult;

/**
 * The Object[] must always be of size two.
 * The first element must be a RegionMapping.
 * The second element must be a Boolean that is true if synchronous.
 */
@Experimental
public class ListDriversFunction extends CliFunction<Object[]> {

  ListDriversFunction() {
    super();
  }

  @Override
  public CliFunctionResult executeFunction(FunctionContext<Object[]> context) {
    try {
      List<String> driverNames;
      driverNames = (getDriverJarUtil()).getRegisteredDriverNames();
      return new CliFunctionResult(context.getMemberName(), driverNames,
          createMessageFromDriversList(driverNames));
    } catch (Exception ex) {
      return new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.ERROR,
          ex.getMessage());
    }
  }

  DriverJarUtils getDriverJarUtil() {
    return new DriverJarUtils();
  }

  String createMessageFromDriversList(List<String> driverNames) {
    String message;
    if (driverNames.isEmpty()) {
      message = "No drivers found.";
    } else {
      message = "{" + driverNames.get(0);

      for (int i = 1; i < driverNames.size(); i++) {
        message += ", ";
        message += driverNames.get(i);
      }

      message += "}";
    }
    return message;
  }
}
