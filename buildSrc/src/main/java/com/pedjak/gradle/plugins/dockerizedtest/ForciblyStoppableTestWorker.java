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

package com.pedjak.gradle.plugins.dockerizedtest;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import org.gradle.api.internal.tasks.testing.WorkerTestClassProcessorFactory;
import org.gradle.api.internal.tasks.testing.worker.TestWorker;

// DHE:
// - Overrides TestWorker.stop() to call Runtime.halt() if it doesn't shut down within 60 seconds.
// - What makes this necessary?
// - Instantiated by custom ForkingTestClassProcessor
// - I think this gets serialized and sent to the test JVM
public class ForciblyStoppableTestWorker extends TestWorker {
  private static final int SHUTDOWN_TIMEOUT = 60; // secs

  public ForciblyStoppableTestWorker(WorkerTestClassProcessorFactory factory) {
    super(factory);
  }

  @Override
  public void stop() {
    new Timer(true).schedule(new TimerTask() {
      @Override
      public void run() {
        System.err.println("Worker process did not shutdown gracefully within " + SHUTDOWN_TIMEOUT
            + "s, forcing it now");
        Runtime.getRuntime().halt(-100);
      }
    }, TimeUnit.SECONDS.toMillis(SHUTDOWN_TIMEOUT));
    super.stop();
  }
}
