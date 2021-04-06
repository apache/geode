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
package org.apache.geode.gradle.testing.isolation

import org.apache.geode.gradle.testing.Executers
import org.apache.geode.gradle.testing.Workers
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.tasks.testing.Test
import org.gradle.internal.remote.MessagingServer
import org.gradle.process.internal.worker.WorkerProcessFactory

import javax.inject.Inject

class IsolatedTestPlugin implements Plugin<Project> {
    static def gradleWorkerProcessFactory
    static def gradleMessagingServer
    static def portRangeWorkerProcessFactory

    @Inject
    IsolatedTestPlugin(MessagingServer gradleMessagingServer,
                       WorkerProcessFactory gradleWorkerProcessFactory) {
        initializeGradleWorkerProcessFactory(gradleWorkerProcessFactory)
        initializeGradleMessagingServer(gradleMessagingServer)
    }

    @Override
    void apply(Project project) {
        initializePortRangeWorkerProcessFactory(project.gradle.startParameter.maxWorkerCount)

        def usePortRangeTestWorker = {
            if (!it.hasProperty('isolatedTest')) {
                return
            }
            it.doFirst {
                testExecuter = Executers.withFactory(it, portRangeWorkerProcessFactory)
            }
        }

        project.tasks.withType(Test).each(usePortRangeTestWorker)
        project.tasks.whenTaskAdded() {
            if (it instanceof Test) {
                it.configure(usePortRangeTestWorker)
            }
        }
    }

    synchronized static initializePortRangeWorkerProcessFactory(int partitionCount) {
        if (portRangeWorkerProcessFactory != null) {
            return
        }
        portRangeWorkerProcessFactory = Workers.createWorkerProcessFactory(
            gradleWorkerProcessFactory,
            new PortRangeProcessLauncher(partitionCount, new WorkingDirectoryIsolator()),
            gradleMessagingServer
        )
    }

    synchronized static initializeGradleMessagingServer(server) {
        if (!gradleMessagingServer) {
            gradleMessagingServer = server
        }
    }

    synchronized static void initializeGradleWorkerProcessFactory(factory) {
        if (!gradleWorkerProcessFactory) {
            gradleWorkerProcessFactory = factory
        }
    }
}
