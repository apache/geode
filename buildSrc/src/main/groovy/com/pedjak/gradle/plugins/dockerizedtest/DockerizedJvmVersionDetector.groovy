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

import com.github.dockerjava.api.model.Frame
import com.github.dockerjava.api.model.StreamType
import com.github.dockerjava.core.command.AttachContainerResultCallback
import com.github.dockerjava.core.command.WaitContainerResultCallback
import com.sun.xml.internal.messaging.saaj.util.ByteOutputStream
import org.gradle.api.GradleException
import org.gradle.api.JavaVersion
import org.gradle.api.logging.Logger
import org.gradle.api.logging.Logging
import org.gradle.internal.jvm.JavaInfo
import org.gradle.internal.jvm.inspection.JvmVersionDetector

import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock
import java.util.regex.Matcher
import java.util.regex.Pattern

class DockerizedJvmVersionDetector implements JvmVersionDetector {
    private static final Logger LOGGER = Logging.getLogger(DockerizedJvmVersionDetector.class);

    private static final Lock lock = new ReentrantLock()

    final DockerizedTestExtension testExtension

    private static final imageJvms = [:]

    DockerizedJvmVersionDetector(DockerizedTestExtension testExtension) {
        this.testExtension = testExtension
    }

    @Override
    JavaVersion getJavaVersion(JavaInfo javaInfo) {
        getVersion()
    }

    @Override
    JavaVersion getJavaVersion(String s) {
        getVersion()
    }


    def getVersion() {
        for (int i = 0; i < 10; i++) {
            lock.lock()
            try {
                def v = imageJvms[(testExtension.image)]
                if (!v) {
                    def client = testExtension.client
                    def createCmd = client.createContainerCmd(testExtension.image.toString())
                            .withTty(false)
                            .withCmd(['java', '-version'])

                    def containerId = createCmd.exec().id
                    client.startContainerCmd(containerId).exec()
                    def w = new ByteOutputStream()
                    client.attachContainerCmd(containerId)
                            .withFollowStream(true)
                            .withStdErr(true)
                            .exec(new AttachContainerResultCallback() {

                        void onNext(Frame frame) {
                            try {
                                if (frame.streamType == StreamType.STDERR) {
                                    w.write(frame.getPayload());
                                }
                            } catch (Exception e) {
                                LOGGER.error("Error while writing to stream:", e);
                            }
                            super.onNext(frame);
                        }
                    })
                    client.waitContainerCmd(containerId).exec(new WaitContainerResultCallback()).awaitStatusCode()
                    client.removeContainerCmd(containerId).withForce(true).exec()

                    // copied from org.gradle.internal.jvm.inspection.DefaultJvmVersionDetector.parseJavaVersionCommandOutput
                    def reader = new BufferedReader(new StringReader(new String(w.getBytes())))

                    String versionStr = reader.readLine();
                    while (versionStr != null) {
                        Matcher matcher = Pattern.compile("(?:java|openjdk) version \"(.+?)\"( \\d{4}-\\d{2}-\\d{2}( LTS)?)?").matcher(versionStr);
                        if (matcher.matches()) {
                            v = JavaVersion.toVersion(matcher.group(1));
                            imageJvms[(testExtension.image)] = v
                            return v
                        }
                        versionStr = reader.readLine();
                    }


                } else {
                    return v
                }
            } finally {
                lock.unlock()
            }
        }
        throw new GradleException("Could not determine Java version in Docker image ${testExtension.image} ")

    }
}
