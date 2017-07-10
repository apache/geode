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
package org.apache.geode.internal.process;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public abstract class BaseProcessStreamReaderIntegrationTest
    extends AbstractProcessStreamReaderIntegrationTest {

  @Test
  public void processLivesAfterClosingStreams() throws Exception {
    // arrange
    process = new ProcessBuilder(createCommandLine(ProcessSleeps.class)).start();

    // act
    process.getErrorStream().close();
    process.getOutputStream().close();
    process.getInputStream().close();

    // assert
    assertThat(process.isAlive()).isTrue();
  }

  @Test
  public void processTerminatesWhenDestroyed() throws Exception {
    // arrange
    process = new ProcessBuilder(createCommandLine(ProcessSleeps.class)).start();
    assertThat(process.isAlive()).isTrue();

    // act
    process.destroy();

    // assert
    await().until(() -> assertThat(process.isAlive()).isFalse());
    assertThat(process.exitValue()).isNotEqualTo(0);
  }
}
