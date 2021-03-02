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

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.util.DriverJarUtil;
import org.apache.geode.management.cli.CliFunction;
import org.apache.geode.management.internal.functions.CliFunctionResult;

/**
 * The Object[] must always be of size two.
 * The first element must be a RegionMapping.
 * The second element must be a Boolean that is true if synchronous.
 */
@Experimental
public class DeregisterDriverFunction extends CliFunction<Object[]> {

  DeregisterDriverFunction() {
    super();
  }

  @Override
  public CliFunctionResult executeFunction(FunctionContext<Object[]> context) {
    try {
      String driverClassName = (String) context.getArguments()[0];
      DriverJarUtil util = getDriverJarUtil();
      util.deregisterDriver(driverClassName);
      return new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.OK,
          driverClassName + " was successfully deregistered.");
    } catch (Exception ex) {
      return new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.ERROR,
          ex.getMessage());
    }
  }

  DriverJarUtil getDriverJarUtil() {
    return new DriverJarUtil();
  }
}
