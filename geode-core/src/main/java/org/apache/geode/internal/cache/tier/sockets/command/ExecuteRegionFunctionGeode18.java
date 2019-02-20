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
package org.apache.geode.internal.cache.tier.sockets.command;


import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.internal.cache.execute.AbstractExecution;
import org.apache.geode.internal.cache.tier.Command;

/**
 * @since Geode 1.8
 */
public class ExecuteRegionFunctionGeode18 extends ExecuteRegionFunction66 {

  @Immutable
  private static final ExecuteRegionFunctionGeode18 singleton = new ExecuteRegionFunctionGeode18();

  public static Command getCommand() {
    return singleton;
  }

  private ExecuteRegionFunctionGeode18() {}

  @Override
  void executeFunctionWithResult(Object function, byte functionState,
      Function<?> functionObject, AbstractExecution execution) {
    if (function instanceof String) {
      switch (functionState) {
        case AbstractExecution.NO_HA_HASRESULT_NO_OPTIMIZEFORWRITE:
          execution.setWaitOnExceptionFlag(true);
          execution.execute((String) function).getResult();
          break;
        case AbstractExecution.HA_HASRESULT_NO_OPTIMIZEFORWRITE:
          execution.execute((String) function).getResult();
          break;
        case AbstractExecution.HA_HASRESULT_OPTIMIZEFORWRITE:
          execution.execute((String) function).getResult();
          break;
        case AbstractExecution.NO_HA_HASRESULT_OPTIMIZEFORWRITE:
          execution.setWaitOnExceptionFlag(true);
          execution.execute((String) function).getResult();
          break;
      }
    } else {
      if (!functionObject.isHA()) {
        execution.setWaitOnExceptionFlag(true);
      }
      execution.execute(functionObject).getResult();
    }
  }

}
