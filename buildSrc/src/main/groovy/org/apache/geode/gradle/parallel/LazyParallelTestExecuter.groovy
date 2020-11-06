/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 *
 */

package org.apache.geode.gradle.parallel

import org.gradle.StartParameter
import org.gradle.api.internal.DocumentationRegistry
import org.gradle.api.internal.tasks.testing.JvmTestExecutionSpec
import org.gradle.api.internal.tasks.testing.TestExecuter
import org.gradle.api.internal.tasks.testing.TestResultProcessor
import org.gradle.api.internal.tasks.testing.filter.DefaultTestFilter
import org.gradle.api.tasks.testing.Test
import org.gradle.internal.operations.BuildOperationExecutor
import org.gradle.internal.service.ServiceRegistry
import org.gradle.internal.time.Clock
import org.gradle.internal.work.WorkerLeaseRegistry

/**
 * A {@link TestExecuter) that defers creating a real {@link ParallelTestExecuter} until
 *{@link #execute} is called.
 */
class LazyParallelTestExecuter implements TestExecuter<JvmTestExecutionSpec> {
  private final Test test
  private TestExecuter<JvmTestExecutionSpec> executer

  LazyParallelTestExecuter(Test test) { this.test = test }

  @Override
  void execute(JvmTestExecutionSpec testExecutionSpec, TestResultProcessor testResultProcessor) {
    executer().execute(testExecutionSpec, testResultProcessor)
  }

  @Override
  void stopNow() {
    if (executer == null) {
      return
    }
    executer.stopNow()
    executer = null
  }

  TestExecuter<JvmTestExecutionSpec> executer() {
    if (executer == null) {
      ServiceRegistry services = test.getServices()

      executer = new ParallelTestExecuter(
          test.getProcessBuilderFactory(),
          test.getActorFactory(),
          test.getModuleRegistry(),
          services.get(WorkerLeaseRegistry.class),
          services.get(BuildOperationExecutor.class),
          services.get(StartParameter.class).getMaxWorkerCount(),
          services.get(Clock.class),
          services.get(DocumentationRegistry.class),
          (DefaultTestFilter) test.getFilter())
    }
    return executer
  }
}
