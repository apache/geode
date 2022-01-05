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

import java.util.concurrent.atomic.AtomicLong;

import org.gradle.api.internal.tasks.testing.DefaultTestDescriptor;
import org.gradle.api.internal.tasks.testing.TestCompleteEvent;
import org.gradle.api.internal.tasks.testing.TestDescriptorInternal;
import org.gradle.api.internal.tasks.testing.TestResultProcessor;
import org.gradle.api.internal.tasks.testing.TestStartEvent;
import org.gradle.api.tasks.testing.TestOutputEvent;

/**
 * A test result processor that identifies which instance of a test class produced each test event,
 * allowing Gradle to direct each test class's output to its own log.
 */
public class RepeatableTestResultProcessor implements TestResultProcessor {
  private static final AtomicLong ID_GENERATOR = new AtomicLong();
  private final long id;
  private final TestResultProcessor processor;

  public RepeatableTestResultProcessor(TestResultProcessor processor) {
    this.processor = processor;
    id = ID_GENERATOR.getAndIncrement();
  }

  @Override
  public void started(TestDescriptorInternal test, TestStartEvent event) {
    processor.started(repeatableDescriptor(test), event);
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

  /**
   * Appends this result processor's ID onto the class name
   */
  private String repeatableClassName(String className) {
    if (className == null) {
      return null;
    }
    return String.format("%s-%d", className, id);
  }

  private TestDescriptorInternal repeatableDescriptor(TestDescriptorInternal descriptor) {
    if (descriptor.getClassName() == null) {
      return descriptor;
    }
    return new DefaultTestDescriptor(descriptor.getId(),
        repeatableClassName(descriptor.getClassName()), descriptor.getName(),
        descriptor.getClassDisplayName(), descriptor.getDisplayName());

  }
}
