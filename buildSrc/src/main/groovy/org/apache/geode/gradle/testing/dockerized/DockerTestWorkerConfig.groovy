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

import org.gradle.api.Project

class DockerTestWorkerConfig {
    static long durationWarningThreshold = 60_000
    String image
    String javaHome
    String localUserID
    String name
    int timeoutMillis = 300_000
    String user
    Map<String, String> volumes = new HashMap<>()

    DockerTestWorkerConfig(Project project) {
        name = project.path
        image = project.dunitDockerImage
        user = project.dunitDockerUser

        if (project.hasProperty('dunitDockerJVM') && !project.dunitDockerJVM.trim().isEmpty()) {
            javaHome = project.dunitDockerJVM as String
        } else if (project.hasProperty('testJVM') && !project.testJVM.trim().isEmpty()) {
            javaHome = project.testJVM as String
        }

        // Mount the user's Gradle home dir, the Geode project root directory, and any
        // user-specified volumes.
        def gradleUserHomeDir = project.gradle.startParameter.gradleUserHomeDir.getAbsolutePath() as String
        def geodeDir = new File(System.getenv('PWD')).getCanonicalPath()
        volumes = [(geodeDir)         : geodeDir,
                   (gradleUserHomeDir): gradleUserHomeDir]

        if (project.hasProperty('dunitDockerVolumes')) {
            volumes.putAll(project.dunitDockerVolumes)
        }

        if (project.hasProperty("dunitDockerTimeout")) {
            timeoutMillis = Integer.parseUnsignedInt(project.dunitDockerTimeout)
        }

        // Unfortunately this snippet of code is here and is required by
        // dev-tools/docker/base/entrypoint.sh. This allows preserving the outer user inside the
        // running container. Required for Jenkins and other environments. There doesn't seem to be
        // a way to pass this environment variable in from a Jenkins Gradle job.
        if (System.env['LOCAL_USER_ID'] == null) {
            def username = System.getProperty("user.name")
            localUserID = ['id', '-u', username].execute().text.trim() as String
        }
    }

    /**
     * Adjust the process builder's command and environment to run in a Docker container.
     */
    def dockerize(processBuilder) {
        def command = processBuilder.command()
        def environment = processBuilder.environment()

        // The JAVA_HOME and PATH environment variables set by Gradle are meaningless inside a
        // Docker container. Remove them.
        if (environment['JAVA_HOME']) {
            environment.remove 'JAVA_HOME'
            environment['JAVA_HOME_REMOVED'] = ""
        }
        if (environment['PATH']) {
            environment.remove 'PATH'
            environment['PATH_REMOVED'] = ""
        }

        if (javaHome) {
            environment['JAVA_HOME'] = javaHome
            command.set(0, "${javaHome}/bin/java" as String)
        }

        if (localUserID) {
            environment['LOCAL_USER_ID'] = localUserID
        }
    }
}
