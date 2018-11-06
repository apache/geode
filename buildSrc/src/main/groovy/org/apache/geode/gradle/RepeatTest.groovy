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
package org.apache.geode.gradle

import org.gradle.StartParameter
import org.gradle.api.file.FileTree
import org.gradle.api.internal.DocumentationRegistry
import org.gradle.api.internal.tasks.testing.JvmTestExecutionSpec
import org.gradle.api.internal.tasks.testing.TestExecuter
import org.gradle.api.internal.tasks.testing.detection.DefaultTestExecuter
import org.gradle.api.internal.tasks.testing.filter.DefaultTestFilter
import org.gradle.api.tasks.testing.Test
import org.gradle.internal.operations.BuildOperationExecutor
import org.gradle.internal.time.Clock
import org.gradle.internal.work.WorkerLeaseRegistry

class RepeatTest extends Test {
  int times = 1

  @Override
  FileTree getCandidateClassFiles() {
    FileTree candidates = super.getCandidateClassFiles()
    int additionalRuns = times - 1
    for (int i = 0; i < additionalRuns; i++) {
      candidates = candidates.plus(super.getCandidateClassFiles())
    }

    return candidates
  }

  /*
   * We have to override gradles default test executor, because that uses {@link RunPreviousFailedFirstTestClassProcessor}
   * Which deduplicates the test specs we're passing in
   */
  @Override
  protected TestExecuter<JvmTestExecutionSpec> createTestExecuter() {
    def oldExecutor = super.createTestExecuter()

    //Use the previously set worker process factory. If the test is
    //being run using the parallel docker plugin, this will be a docker
    //process factory
    def workerProcessFactory = oldExecutor.workerFactory

    return new OverriddenTestExecutor(workerProcessFactory, getActorFactory(),
        getModuleRegistry(),
        getServices().get(WorkerLeaseRegistry.class),
        getServices().get(BuildOperationExecutor.class),
        getServices().get(StartParameter.class).getMaxWorkerCount(),
        getServices().get(Clock.class),
        getServices().get(DocumentationRegistry.class),
        (DefaultTestFilter) getFilter())
  }

}
