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

import org.gradle.api.Project
import org.gradle.api.tasks.testing.Test

import java.util.concurrent.Semaphore

// DHE:
// - Why not use Gradle's lease mechanism? Answer: Gradle's WorkerLeaseRegistry was added in 2017,
//   after this class was created.
class DefaultWorkerSemaphore implements WorkerSemaphore {
    private int maxWorkers = Integer.MAX_VALUE
    private Semaphore semaphore
    private logger

    @Override
    void acquire() {
        semaphore().acquire()
        logger.debug("Semaphore acquired, available: {}/{}", semaphore().availablePermits(), maxWorkers)
    }

    @Override
    void release() {
        semaphore().release()
        logger.debug("Semaphore released, available: {}/{}", semaphore().availablePermits(), maxWorkers)
    }

    @Override
    synchronized void applyTo(Project project) {
        if (semaphore) return
        if (!logger) {
            logger = project.logger
        }

        // DHE: Set maxWorkers to the smallest maxParallelForks in any dockerized test task
        maxWorkers = project.tasks.withType(Test).findAll {
            it.extensions.docker?.image != null
        }.collect {
            def v = it.maxParallelForks
            it.maxParallelForks = 10000
            v
        }.min() ?: 1
        semaphore()
    }

    private synchronized setMaxWorkers(int num) {
        if (this.@maxWorkers > num) {
            this.@maxWorkers = num
        }
    }

    private synchronized Semaphore semaphore() {
        if (semaphore == null) {
            semaphore = new Semaphore(maxWorkers)
            logger.lifecycle("Do not allow more than {} test workers", maxWorkers)
        }
        semaphore
    }
}
