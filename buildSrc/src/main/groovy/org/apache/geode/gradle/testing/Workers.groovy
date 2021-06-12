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

import org.apache.geode.gradle.testing.process.LauncherProxyWorkerProcessFactory
import org.apache.geode.gradle.testing.process.ProcessLauncher
import org.gradle.internal.remote.MessagingServer
import org.gradle.process.internal.worker.WorkerProcessFactory

class Workers {
    /**
     * Creates a worker process factory that borrows most components from the donor, but uses the
     * given process launcher and messaging server to launch worker processes.
     */
    static WorkerProcessFactory createWorkerProcessFactory(
            WorkerProcessFactory donor,
            ProcessLauncher processLauncher,
            MessagingServer messagingServer) {
        def workerImplementationFactory = donor.workerImplementationFactory
        return new LauncherProxyWorkerProcessFactory(
                donor.loggingManager,
                messagingServer,
                workerImplementationFactory.classPathRegistry,
                donor.idGenerator,
                workerImplementationFactory.gradleUserHomeDir,
                workerImplementationFactory.temporaryFileProvider,
                donor.execHandleFactory,
                workerImplementationFactory.jvmVersionDetector,
                donor.outputEventListener,
                donor.memoryManager,
                processLauncher)
    }
}
