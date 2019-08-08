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
package org.apache.geode.internal.logging;

import static org.apache.geode.logging.internal.spi.LogWriterLevel.CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.io.OutputStream;
import java.io.PrintStream;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.LoggingTest;

/** Unit tests for {@link org.apache.geode.internal.logging.ManagerLogWriter}. */
@Category(LoggingTest.class)
public class ManagerLogWriterTest {

  @Test
  public void logWriterLevelIsPassedIntoConstructor() {
    ManagerLogWriter logWriter = new ManagerLogWriter(CONFIG.intLevel(),
        new PrintStream(mock(OutputStream.class)), true);

    assertThat(logWriter.getLogWriterLevel()).isEqualTo(CONFIG.intLevel());
  }
}
