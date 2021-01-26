/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pedjak.gradle.plugins.dockerizedtest

import com.github.dockerjava.api.DockerClient
import com.github.dockerjava.core.DefaultDockerClientConfig
import com.github.dockerjava.core.DockerClientBuilder
import com.github.dockerjava.netty.NettyDockerCmdExecFactory
import org.apache.commons.lang3.SystemUtils
import org.apache.maven.artifact.versioning.ComparableVersion
import org.gradle.api.Action
import org.gradle.api.GradleException
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.internal.file.DefaultFileCollectionFactory
import org.gradle.api.tasks.testing.Test
import org.gradle.initialization.DefaultBuildCancellationToken
import org.gradle.internal.concurrent.DefaultExecutorFactory
import org.gradle.internal.concurrent.ExecutorFactory
import org.gradle.internal.operations.BuildOperationExecutor
import org.gradle.internal.remote.Address
import org.gradle.internal.remote.ConnectionAcceptor
import org.gradle.internal.remote.MessagingServer
import org.gradle.internal.remote.ObjectConnection
import org.gradle.internal.remote.internal.ConnectCompletion
import org.gradle.internal.remote.internal.IncomingConnector
import org.gradle.internal.remote.internal.hub.MessageHubBackedObjectConnection
import org.gradle.internal.remote.internal.inet.MultiChoiceAddress
import org.gradle.internal.time.Clock
import org.gradle.process.internal.JavaExecHandleFactory
import org.gradle.process.internal.worker.DefaultWorkerProcessFactory

import javax.inject.Inject

/**
 * DHE:
 * - Configures each test task to run each test worker in a separate Docker container.
 */
class DockerizedTestPlugin implements Plugin<Project> {

    // DHE: minimum supported Gradle version. Perhaps we should also constrain the maximum version.
    def supportedVersion = '4.8'
    def currentUser
    def messagingServer
    // DHE: Used by the ExitCodeTolerantExecHandle... for what?
    def static workerSemaphore = new DefaultWorkerSemaphore()
    // DHE: What is the purpose of this?
    def memoryManager = new com.pedjak.gradle.plugins.dockerizedtest.NoMemoryManager()

    @Inject
    DockerizedTestPlugin(MessagingServer messagingServer) {
        this.currentUser = SystemUtils.IS_OS_WINDOWS ? "0" : "id -u".execute().text.trim()
        this.messagingServer = new MessageServer(messagingServer.connector, messagingServer.executorFactory)
    }

    void configureTest(project, test) {
        def ext = test.extensions.create("docker", DockerizedTestExtension, [] as Object[])
        def startParameter = project.gradle.startParameter
        ext.volumes = ["$startParameter.gradleUserHomeDir": "$startParameter.gradleUserHomeDir",
                       "$project.projectDir"              : "$project.projectDir"]
        ext.user = currentUser
        test.doFirst {
            def extension = test.extensions.docker

            if (extension?.image) {
                // DHE: ????
                workerSemaphore.applyTo(test.project)

                // DHE: Assign a custom test executer that launches test workers in Docker
                test.testExecuter = new com.pedjak.gradle.plugins.dockerizedtest.TestExecuter(newProcessBuilderFactory(project, extension, test.processBuilderFactory), actorFactory, moduleRegistry, services.get(BuildOperationExecutor), services.get(Clock));

                // DHE: Launch a docker client if it isn't already assigned
                if (!extension.client) {
                    // DHE: If we give each test task its own client, would the client already be
                    // assigned at this point?
                    extension.client = createDefaultClient()
                }
            }

        }
    }

    DockerClient createDefaultClient() {
        DockerClientBuilder.getInstance(DefaultDockerClientConfig.createDefaultConfigBuilder())
                .withDockerCmdExecFactory(new NettyDockerCmdExecFactory())
                .build()
    }

    void apply(Project project) {

        boolean unsupportedVersion = new ComparableVersion(project.gradle.gradleVersion).compareTo(new ComparableVersion(supportedVersion)) < 0
        if (unsupportedVersion) throw new GradleException("dockerized-test plugin requires Gradle ${supportedVersion}+")

        // DHE: Configure all existing test tasks
        project.tasks.withType(Test).each { test -> configureTest(project, test) }

        // DHE: Arrange to configure subsequently added test tasks
        project.tasks.whenTaskAdded { task ->
            if (task instanceof Test) configureTest(project, task)
        }
    }

    def newProcessBuilderFactory(project, extension, defaultProcessBuilderFactory) {

        def executorFactory = new DefaultExecutorFactory()
        def executor = executorFactory.create("Docker container link")
        def buildCancellationToken = new DefaultBuildCancellationToken()

        def defaultfilecollectionFactory = new DefaultFileCollectionFactory(project.fileResolver, null)

        // DHE: Create an exec handle factory that creates Dockerized exec handles
        def execHandleFactory = [newJavaExec: { ->
            new DockerizedJavaExecHandleBuilder(
              extension, project.fileResolver, defaultfilecollectionFactory,
              executor, buildCancellationToken, workerSemaphore)
        }] as JavaExecHandleFactory

        // DHE: Return a default worker process factory that gets most details from the given
        // factory, but uses our custom exec handle factory, messaging server, and memory manager.
        new DefaultWorkerProcessFactory(defaultProcessBuilderFactory.loggingManager,
                messagingServer,
                defaultProcessBuilderFactory.workerImplementationFactory.classPathRegistry,
                defaultProcessBuilderFactory.idGenerator,
                defaultProcessBuilderFactory.gradleUserHomeDir,
                defaultProcessBuilderFactory.workerImplementationFactory.temporaryFileProvider,
                execHandleFactory,
                defaultProcessBuilderFactory.workerImplementationFactory.jvmVersionDetector,
                defaultProcessBuilderFactory.outputEventListener,
                memoryManager
        )
    }

    /**
     * DHE: Modification of Gradle's MessageHubBackedServer v5.5
     * - Modification: Insist on non-loopback addresses (where Gradle's implementation prefers
     *   loopback addresses)
     * - Move this and the associated classes into separate (java?) files
     */
    class MessageServer implements MessagingServer {
        def IncomingConnector connector;
        def ExecutorFactory executorFactory;

        public MessageServer(IncomingConnector connector, ExecutorFactory executorFactory) {
            this.connector = connector;
            this.executorFactory = executorFactory;
        }

        public ConnectionAcceptor accept(Action<ObjectConnection> action) {
            return new ConnectionAcceptorDelegate(connector.accept(new ConnectEventAction(action, executorFactory), true))
        }
    }

    /**
     * DHE:
     * - Seems the same as MessageHubBackedServer.ConnectEventAction, except that it is not an
     *   inner class of the server class that uses it.
     */
    class ConnectEventAction implements Action<ConnectCompletion> {
        def action;
        def executorFactory;

        public ConnectEventAction(Action<ObjectConnection> action, executorFactory) {
            this.executorFactory = executorFactory
            this.action = action
        }

        public void execute(ConnectCompletion completion) {
            action.execute(new MessageHubBackedObjectConnection(executorFactory, completion));
        }
    }

    /**
     * DHE:
     * - Wraps Gradle's TcpIncomingConnector
     * - TcpIncomingConnector prefers loopback addresses
     * - This class insists on non-loopback addresses, and so replaces the address's candidates
     *   (which are likely all loopback addresses) with non-loopback ones.
     */
    class ConnectionAcceptorDelegate implements ConnectionAcceptor {

        MultiChoiceAddress address

        @Delegate
        ConnectionAcceptor delegate

        ConnectionAcceptorDelegate(ConnectionAcceptor delegate) {
            this.delegate = delegate
        }

        Address getAddress() {
            synchronized (delegate)
            {
                if (address == null) {
                    // DHE: Maybe use new InetAddresses().getRemote()
                    def remoteAddresses = NetworkInterface.networkInterfaces.findAll {
                        try {
                            return it.up && !it.loopback
                        } catch (SocketException ex) {
                            logger.warn("Unable to inspect interface " + it)
                            return false
                        }
                    }*.inetAddresses*.collect { it }.flatten()
                    def original = delegate.address
                    address = new MultiChoiceAddress(original.canonicalAddress, original.port, remoteAddresses)
                }
            }
            address
        }
    }

}
