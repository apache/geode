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
package org.apache.geode.metrics.function.executions;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.toList;

import java.util.Collection;
import java.util.List;

import io.micrometer.core.instrument.Timer;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.metrics.SimpleMetricsPublishingService;

public class GetFunctionExecutionTimerValues implements Function<Void> {
  private static final String ID = "GetFunctionExecutionTimerValues";

  @Override
  public void execute(FunctionContext<Void> context) {
    Collection<Timer> timers = SimpleMetricsPublishingService.getRegistry()
        .find("geode.function.executions")
        .timers();

    List<ExecutionsTimerValues> result = timers.stream()
        .map(GetFunctionExecutionTimerValues::toExecutionsTimerValues)
        .filter(t -> !t.functionId.equals(ID))
        .collect(toList());

    context.getResultSender().lastResult(result);
  }

  @Override
  public String getId() {
    return ID;
  }

  private static ExecutionsTimerValues toExecutionsTimerValues(Timer t) {
    String functionId = t.getId().getTag("function");
    boolean succeeded = Boolean.parseBoolean(t.getId().getTag("succeeded"));

    return new ExecutionsTimerValues(
        functionId,
        succeeded,
        t.count(),
        t.totalTime(NANOSECONDS));
  }
}
