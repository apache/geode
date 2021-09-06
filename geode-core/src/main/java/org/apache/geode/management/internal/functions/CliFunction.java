/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.management.internal.functions;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.internal.exceptions.EntityNotFoundException;

/**
 * An abstract function implementation to be extended by cli functions. Any cli function extending
 * this class has to return a CliFunctionResult.
 */
public abstract class CliFunction<T> implements InternalFunction<T> {
  private static final Logger logger = LogService.getLogger();

  @Override
  public final void execute(FunctionContext<T> context) {
    try {
      context.getResultSender().lastResult(executeFunction(context));
    } catch (EntityNotFoundException nfe) {
      context.getResultSender().lastResult(new CliFunctionResult(context.getMemberName(), nfe));
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      context.getResultSender().lastResult(new CliFunctionResult(context.getMemberName(), e));
    }
  }

  @Override
  public boolean isHA() {
    return false;
  }

  public abstract CliFunctionResult executeFunction(FunctionContext<T> context) throws Exception;
}
