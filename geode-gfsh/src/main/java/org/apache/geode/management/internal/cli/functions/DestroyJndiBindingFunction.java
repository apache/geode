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
package org.apache.geode.management.internal.cli.functions;

import javax.naming.NamingException;

import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.jndi.JNDIInvoker;
import org.apache.geode.management.cli.CliFunction;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.i18n.CliStrings;

public class DestroyJndiBindingFunction extends CliFunction<Object[]> {

  private static final String ID =
      "org.apache.geode.management.internal.cli.functions.DestroyJndiBindingFunction";

  @Override
  public String getId() {
    return ID;
  }

  @Override
  public CliFunctionResult executeFunction(FunctionContext<Object[]> context) {
    String jndiName = (String) context.getArguments()[0];
    boolean destroyingDataSource = (boolean) context.getArguments()[1];

    String typeName = "Jndi binding";

    if (destroyingDataSource) {
      typeName = "Data source";
      if (!isValidDataSource(jndiName)) {
        return new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.ERROR,
            CliStrings.format(
                "Data Source {0} has invalid type for destroy data-source, destroy jndi-binding command should be used.",
                jndiName));
      }
    }

    final String RESULT_MESSAGE = "{0} \"{1}\" destroyed on \"{2}\"";
    final String EXCEPTION_RESULT_MESSAGE = "{0} \"{1}\" not found on \"{2}\"";

    try {
      JNDIInvoker.unMapDatasource(jndiName);
      return new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.OK,
          CliStrings.format(RESULT_MESSAGE, typeName, jndiName, context.getMemberName()));
    } catch (NamingException e) {
      return new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.OK,
          CliStrings.format(EXCEPTION_RESULT_MESSAGE, typeName, jndiName, context.getMemberName()));
    }
  }

  boolean isValidDataSource(String jndiName) {
    return JNDIInvoker.isValidDataSource(jndiName);
  }
}
