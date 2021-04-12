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
package org.apache.geode.gradle.testing.dockerized


import org.apache.geode.gradle.testing.Executers
import org.apache.geode.gradle.testing.Workers
import org.apache.geode.gradle.testing.isolation.WorkingDirectoryIsolator
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.tasks.testing.Test
import org.gradle.internal.remote.MessagingServer
import org.gradle.internal.remote.internal.inet.TcpIncomingConnector
import org.gradle.process.internal.worker.WorkerProcessFactory

import javax.inject.Inject

/**
 * Extends each test task with a configuration that it can use to launch its test workers in Docker
 * containers. The <Geode project root>/gradle/multi-process-test.gradle file defines which test
 * tasks actually apply the configuration to run test workers.
 */
class DockerizedTestPlugin implements Plugin<Project> {
    /**
     * A custom {@link MessagingServer} that supports communication between Gradle and processes
     * running in Docker containers.
     */
    def static dockerMessagingServer
    /**
     * The singleton {@link MessagingServer} created by Gradle. This plugin borrows the server's
     * internal components to build a custom {@code MessagingServer} that can communicate with
     * processes running in Docker containers.
     */
    def static gradleMessagingServer
    /**
     * The singleton {@link WorkerProcessFactory} created by Gradle. This plugin borrows the
     * factory's internal components to build a custom {@code WorkerProcessFactory} that launches
     * processes in Docker containers.
     */
    def static gradleWorkerProcessFactory

    /**
     * The injected values are singletons. Gradle injects the same instances into each instance of
     * this plugin.
     * <p>
     * CAVEAT: The types of these parameters are declared internal by Gradle v6.8.3. Future
     * versions of Gradle may not include these types, or may change their implementation.
     * <p>
     * CAVEAT: The Gradle v6.8.3 documentation does not list these types among the services that
     * Gradle will inject into plugins. Future versions of Gradle may not inject these values.
     */
    @Inject
    DockerizedTestPlugin(MessagingServer gradleMessagingServer,
                         WorkerProcessFactory gradleWorkerProcessFactory) {
        initializeGradleWorkerProcessFactory(gradleWorkerProcessFactory)
        initializeGradleMessagingServer(gradleMessagingServer)
    }

    @Override
    void apply(Project project) {
        if (!project.hasProperty('parallelDunit')) {
            return
        }

        initializeMessagingServer()

        def dockerTestWorkerConfig = new DockerTestWorkerConfig(project)
        def dockerProcessLauncher = new DockerProcessLauncher(dockerTestWorkerConfig, new WorkingDirectoryIsolator())
        def dockerWorkerProcessFactory = Workers.createWorkerProcessFactory(
                gradleWorkerProcessFactory,
                dockerProcessLauncher,
                dockerMessagingServer)

        def useDockerTestWorker = {
            it.doFirst {
                testExecuter = Executers.withFactory(it, dockerWorkerProcessFactory)
            }
        }

        project.tasks.withType(Test).each(useDockerTestWorker)
        project.tasks.whenTaskAdded() {
            if (it instanceof Test) {
                it.configure(useDockerTestWorker)
            }
        }
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

    synchronized static void initializeMessagingServer() {
        if (dockerMessagingServer) {
            return
        }

        def gradleConnector = gradleMessagingServer.connector
        def gradleExecutorFactory = gradleConnector.executorFactory
        def gradleIdGenerator = gradleConnector.idGenerator

        /**
         * Use a custom {@link WildcardBindingInetAddressFactory} to allow connections from
         * processes in Docker containers.
         */
        def wildcardAddressFactory = new WildcardBindingInetAddressFactory()
        def dockerConnector = new TcpIncomingConnector(
                gradleExecutorFactory,
                wildcardAddressFactory,
                gradleIdGenerator
        )
        /**
         * Use a custom {@link DockerMessagingServer} that yields connection addresses usable
         * by processes in Docker containers.
         */
        dockerMessagingServer = new DockerMessagingServer(dockerConnector, gradleExecutorFactory)
    }
}
