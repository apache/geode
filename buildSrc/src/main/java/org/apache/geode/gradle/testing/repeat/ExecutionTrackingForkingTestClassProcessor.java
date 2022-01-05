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

import org.gradle.api.internal.tasks.testing.TestClassProcessor;
import org.gradle.api.internal.tasks.testing.TestClassRunInfo;
import org.gradle.api.internal.tasks.testing.TestResultProcessor;

/**
 * A test class processor that decorates its result processor to associate each test event with
 * the test class execution that reported it.
 */
public class ExecutionTrackingForkingTestClassProcessor implements TestClassProcessor {
  private final TestClassProcessor processor;
  private final int iterationCount;

  public ExecutionTrackingForkingTestClassProcessor(TestClassProcessor processor,
      int iterationCount) {
    this.processor = processor;
    this.iterationCount = iterationCount;
  }

  @Override
  public void startProcessing(TestResultProcessor resultProcessor) {
    processor.startProcessing(
        new ExecutionTrackingTestResultProcessor(resultProcessor, iterationCount));
  }

  @Override
  public void processTestClass(TestClassRunInfo testClass) {
    processor.processTestClass(testClass);
  }

  @Override
  public void stop() {
    processor.stop();
  }

  @Override
  public void stopNow() {
    processor.stopNow();
  }
}
