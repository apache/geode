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

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.naming.Context;

import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.jndi.JNDIInvoker;
import org.apache.geode.management.cli.CliFunction;
import org.apache.geode.management.internal.functions.CliFunctionResult;

public class ListJndiBindingFunction extends CliFunction<Void> {

  private static final long serialVersionUID = 5254506785395069200L;

  @Override
  public CliFunctionResult executeFunction(FunctionContext<Void> context) {
    CliFunctionResult result;
    try {
      Context ctx = JNDIInvoker.getJNDIContext();
      Map<String, String> bindings = JNDIInvoker.getBindingNamesRecursively(ctx);
      List<String> resultValues = bindings.entrySet().stream()
          .flatMap((e) -> Arrays.stream(new String[] {e.getKey(), e.getValue()}))
          .collect(Collectors.toList());
      result = createCliFunctionResult(context, resultValues);
    } catch (Exception e) {
      result =
          new CliFunctionResult(context.getMemberName(), e, "Unable to retrieve JNDI bindings");
    }
    return result;
  }

  @SuppressWarnings("deprecation")
  private CliFunctionResult createCliFunctionResult(FunctionContext<Void> context,
      List<String> resultValues) {
    return new CliFunctionResult(context.getMemberName(),
        resultValues.toArray(new Serializable[] {}));
  }
}
