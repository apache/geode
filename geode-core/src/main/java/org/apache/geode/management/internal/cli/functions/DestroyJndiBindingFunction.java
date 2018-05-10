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
import org.apache.geode.management.internal.cli.i18n.CliStrings;

public class DestroyJndiBindingFunction extends CliFunction<String> {

  static final String RESULT_MESSAGE = "Jndi binding \"{0}\" destroyed on \"{1}\"";
  static final String EXCEPTION_RESULT_MESSAGE = "Jndi binding \"{0}\" not found on \"{1}\"";

  @Override
  public CliFunctionResult executeFunction(FunctionContext context) {
    String jndiName = (String) context.getArguments();

    try {
      JNDIInvoker.unMapDatasource(jndiName);
      return new CliFunctionResult(context.getMemberName(), true,
          CliStrings.format(RESULT_MESSAGE, jndiName, context.getMemberName()));
    } catch (NamingException e) {
      return new CliFunctionResult(context.getMemberName(), true,
          CliStrings.format(EXCEPTION_RESULT_MESSAGE, jndiName, context.getMemberName()));
    }
  }
}
