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
package org.apache.geode.gradle.testing.repeat;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.gradle.api.internal.tasks.testing.DefaultTestClassDescriptor;
import org.gradle.api.internal.tasks.testing.DefaultTestDescriptor;
import org.gradle.api.internal.tasks.testing.TestCompleteEvent;
import org.gradle.api.internal.tasks.testing.TestDescriptorInternal;
import org.gradle.api.internal.tasks.testing.TestResultProcessor;
import org.gradle.api.internal.tasks.testing.TestStartEvent;
import org.gradle.api.internal.tasks.testing.worker.WorkerTestClassProcessor;
import org.gradle.api.tasks.testing.TestOutputEvent;

/**
 * A test result processor that associates each test event with the test class execution that
 * reported it.
 */
public class ExecutionTrackingTestResultProcessor implements TestResultProcessor {
  private static final Map<String, AtomicInteger> EXECUTION_COUNTERS = new ConcurrentHashMap<>();
  private final TestResultProcessor processor;
  private final String executionNameFormat;
  private String executionName;

  public ExecutionTrackingTestResultProcessor(TestResultProcessor processor, int repetitions) {
    this.processor = processor;
    int idWidth = String.valueOf(repetitions).length();
    executionNameFormat = "%s-%0" + idWidth + 'd';
  }

  /**
   * Reports a test start event, appending an execution ID to the test class name. The execution ID
   * is a simple counter that distinguishes one execution of a given test class from another.
   */
  @Override
  public void started(TestDescriptorInternal test, TestStartEvent event) {
    processor.started(executionTrackingDescriptor(test), event);
  }

  @Override
  public void completed(Object testId, TestCompleteEvent event) {
    processor.completed(testId, event);
  }

  @Override
  public void output(Object testId, TestOutputEvent event) {
    processor.output(testId, event);
  }

  @Override
  public void failure(Object testId, Throwable result) {
    processor.failure(testId, result);
  }

  private TestDescriptorInternal executionTrackingDescriptor(TestDescriptorInternal original) {
    if (original instanceof DefaultTestDescriptor) {
      return new DefaultTestDescriptor(original.getId(), executionName, original.getName(),
          original.getClassDisplayName(), original.getDisplayName());
    }
    if (original instanceof DefaultTestClassDescriptor) {
      executionName = nextExecutionNameFor(original.getClassName());
      return new DefaultTestClassDescriptor(original.getId(), executionName,
          original.getClassDisplayName());
    }
    if (!(original instanceof WorkerTestClassProcessor.WorkerTestSuiteDescriptor)) {
      System.out.printf(
          "WARNING: %s does not recognize test class descriptor type %s (className=%s, name=%s)%n",
          getClass().getName(), original.getClass().getSimpleName(), original.getClassName(),
          original.getName());
    }
    return original;
  }

  private String nextExecutionNameFor(String className) {
    AtomicInteger counter =
        EXECUTION_COUNTERS.computeIfAbsent(className, name -> new AtomicInteger());
    int executionCount = counter.incrementAndGet();
    return String.format(executionNameFormat, className, executionCount);
  }
}
