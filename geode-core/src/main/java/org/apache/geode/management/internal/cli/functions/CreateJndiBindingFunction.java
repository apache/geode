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

import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.internal.jndi.JNDIInvoker;
import org.apache.geode.management.internal.cli.i18n.CliStrings;

public class CreateJndiBindingFunction implements InternalFunction<JndiBindingConfiguration> {

  static final String RESULT_MESSAGE =
      "Initiated jndi binding \"{0}\" on \"{1}\". See server logs to verify.";

  @Override
  public void execute(FunctionContext<JndiBindingConfiguration> context) {
    ResultSender<Object> resultSender = context.getResultSender();
    JndiBindingConfiguration configuration = context.getArguments();
    JNDIInvoker.mapDatasource(configuration.getParamsAsMap(),
        configuration.getDatasourceConfigurations());

    resultSender.lastResult(new CliFunctionResult(context.getMemberName(), true,
        CliStrings.format(RESULT_MESSAGE, configuration.getJndiName(), context.getMemberName())));
  }
}
