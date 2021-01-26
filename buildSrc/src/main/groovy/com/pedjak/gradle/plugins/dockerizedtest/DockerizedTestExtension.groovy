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

/**
 * DHE:
 * - Configuration for a dockerized process.
 * - Instantiated by DockerizedTestPlugin for each appropriate test task
 */
class DockerizedTestExtension {

    String image
    Map volumes
    String user

    Closure beforeContainerCreate

    Closure afterContainerCreate

    Closure beforeContainerStart

    Closure afterContainerStart

    Closure afterContainerStop = { containerId, client ->
        try {
            client.removeContainerCmd(containerId).exec();
        } catch (Exception e) {
            // ignore any error
            // DHE: Why ignore it?
        }
    }

    // could be a DockerClient instance or a closure that returns a DockerClient instance
    // DHE: Delete this if we don't use it directly
    private def clientOrClosure

    void setClient(clientOrClosure) {
        this.clientOrClosure = clientOrClosure
    }

    DockerClient getClient() {
        if (clientOrClosure == null) return null
        if (DockerClient.class.isAssignableFrom(clientOrClosure.getClass())) {
            return (DockerClient) clientOrClosure;
        } else {
            return (DockerClient) ((Closure) clientOrClosure).call();
        }
    }
}
