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
package org.apache.geode.gradle.test.dockerized

import com.github.dockerjava.api.DockerClient
import com.github.dockerjava.core.DefaultDockerClientConfig
import com.github.dockerjava.core.DockerClientBuilder
import com.github.dockerjava.netty.NettyDockerCmdExecFactory
import org.gradle.StartParameter
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.internal.DocumentationRegistry
import org.gradle.api.internal.tasks.testing.TestExecuter
import org.gradle.api.internal.tasks.testing.detection.DefaultTestExecuter
import org.gradle.api.tasks.testing.Test
import org.gradle.internal.remote.MessagingServer
import org.gradle.internal.remote.internal.inet.TcpIncomingConnector
import org.gradle.internal.time.Clock
import org.gradle.internal.work.WorkerLeaseRegistry
import org.gradle.process.internal.worker.WorkerProcessFactory

import javax.inject.Inject

class DockerizedTestPlugin implements Plugin<Project> {
    def static messagingServer
    def static memoryManager = new NoOpMemoryManager()
    def config
    def gradleWorkerProcessFactory
    def workerProcessFactory
    def dockerClient

    /**
     * CAVEAT: Future Gradle may not inject these types:
     * <ul>
     *     <li>{@link MessagingServer}</li>
     *     <li>{@link WorkerProcessFactory}</li>
     * </ul>
     * These are internal Gradle types, and Gradle's documentation does not list them among the
     * services that Gradle will inject into plugins.
     */
    @Inject
    DockerizedTestPlugin(MessagingServer gradleMessagingServer,
                         WorkerProcessFactory gradleWorkerProcessFactory) {
        this.gradleWorkerProcessFactory = gradleWorkerProcessFactory
        if(!messagingServer) {
            def originalConnector = gradleMessagingServer.connector
            def executorFactory = originalConnector.executorFactory
            def wildcardAddressFactory = new WildcardBindingInetAddressFactory()
            def connector = new TcpIncomingConnector(
                    executorFactory,
                    wildcardAddressFactory,
                    originalConnector.idGenerator
            )
            messagingServer = new DockerMessagingServer(connector, executorFactory)
        }
        dockerClient = createDockerClient()
    }

    @Override
    void apply(Project project) {
        if (!project.hasProperty('parallelDunit')) {
            return
        }
        config = project.extensions.create("dockerizedTest", DockerizedTestConfig)
        config.image = project.dunitDockerImage
        config.user = project.dunitDockerUser

        def startParameter = project.gradle.startParameter
        def gradleUserHomeDir = startParameter.gradleUserHomeDir.getAbsolutePath() as String
        def geodeDir = new File(System.getenv('PWD')).getCanonicalPath()
        config.volumes = [(geodeDir)       : geodeDir,
                          (gradleUserHomeDir): gradleUserHomeDir]

        if (project.hasProperty('dunitDockerVolumes')) {
            config.volumes.putAll(project.dunitDockerVolumes)
        }

        def existingTestTasks = project.tasks.withType(Test)
        workerProcessFactory = dockerizedWorkerProcessFactory()
        existingTestTasks.each {
            configureTest(it)
        }
        project.tasks.whenTaskAdded { task ->
            if (task instanceof Test) configureTest(task)
        }
    }

    void configureTest(test) {
        test.doFirst {
            test.testExecuter = dockerizedTestExecuter(test)
        }
    }

    /**
     * Creates a worker process factory borrows most internal components from the one created by
     * Gradle, but replaces the messaging server and process launcher with this plugin's dockerized
     * ones.
     */
    WorkerProcessFactory dockerizedWorkerProcessFactory() {
        def processLauncher = new DockerProcessLauncher(config, dockerClient)
        def workerImplementationFactory = gradleWorkerProcessFactory.workerImplementationFactory
        return new DockerWorkerProcessFactory(
                gradleWorkerProcessFactory.loggingManager,
                messagingServer,
                workerImplementationFactory.classPathRegistry,
                gradleWorkerProcessFactory.idGenerator,
                workerImplementationFactory.gradleUserHomeDir,
                workerImplementationFactory.temporaryFileProvider,
                gradleWorkerProcessFactory.execHandleFactory,
                workerImplementationFactory.jvmVersionDetector,
                gradleWorkerProcessFactory.outputEventListener,
                memoryManager,
                processLauncher);
    }

    /**
     * Creates a test executer that borrows most internal components from the test, but replaces
     * the worker process factory with this plugin's dockerized one.
     * @param donor the original test executer created by Gradle
     * @return a dockerized version of the donor
     */
    TestExecuter dockerizedTestExecuter(test) {
        def services = test.services
        return new DefaultTestExecuter(
                workerProcessFactory,
                test.actorFactory,
                test.moduleRegistry,
                services.get(WorkerLeaseRegistry),
                services.get(StartParameter).getMaxWorkerCount(),
                services.get(Clock),
                services.get(DocumentationRegistry),
                test.filter
        )
    }

    static DockerClient createDockerClient() {
        DockerClientBuilder.getInstance(DefaultDockerClientConfig.createDefaultConfigBuilder())
                .withDockerCmdExecFactory(new NettyDockerCmdExecFactory())
                .build()
    }
}
