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
 *
 */

package org.apache.geode.gradle.testing

import org.apache.geode.gradle.testing.process.AdjustableProcessLauncher
import org.gradle.StartParameter
import org.gradle.api.internal.DocumentationRegistry
import org.gradle.api.internal.tasks.testing.JvmTestExecutionSpec
import org.gradle.api.internal.tasks.testing.TestExecuter
import org.gradle.api.internal.tasks.testing.detection.DefaultTestExecuter
import org.gradle.internal.time.Clock
import org.gradle.internal.work.WorkerLeaseRegistry

class Executers {
    /**
     * Creates a {@code TestExecuter} that applies an adjustment to each test worker
     * {@link ProcessBuilder} just before launching the process.
     */
    static TestExecuter<JvmTestExecutionSpec> withAdjustment(testTask, adjustment) {
        def gradleWorkerProcessFactory = testTask.createTestExecuter().workerFactory
        def messagingServer = gradleWorkerProcessFactory.server
        def launcher = new AdjustableProcessLauncher(adjustment)
        def isolatingWorkerProcessFactory = Workers.createWorkerProcessFactory(
            gradleWorkerProcessFactory,
            launcher,
            messagingServer)
        return withFactory(testTask, isolatingWorkerProcessFactory)
    }

    /**
     * Creates a {@code TestExecuter} that uses the given factory to create test worker processes.
     */
    static TestExecuter<JvmTestExecutionSpec> withFactory(testTask, workerProcessFactory) {
        def services = testTask.services
        return new DefaultTestExecuter(
            workerProcessFactory,
            testTask.actorFactory,
            testTask.moduleRegistry,
            services.get(WorkerLeaseRegistry),
            services.get(StartParameter).getMaxWorkerCount(),
            services.get(Clock),
            services.get(DocumentationRegistry),
            testTask.filter
        )
    }
}
