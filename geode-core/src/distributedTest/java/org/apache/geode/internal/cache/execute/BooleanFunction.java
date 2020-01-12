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

package org.apache.geode.internal.cache.execute;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;

/**
 * Extracted from {@link PRFunctionExecutionDUnitTest}.
 */
class BooleanFunction<Boolean> implements Function<Boolean> {

  private final boolean value;

  BooleanFunction(final boolean value) {
    this.value = value;
  }

  @Override
  public void execute(final FunctionContext context) {
    context.getResultSender().lastResult(value);
  }

  @Override
  public String getId() {
    return getClass().getName();
  }

  @Override
  public boolean hasResult() {
    return true;
  }
}
