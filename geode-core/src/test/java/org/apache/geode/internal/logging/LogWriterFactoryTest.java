/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.logging;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.apache.geode.LogWriter;
import org.apache.geode.logging.internal.log4j.LogWriterLogger;

public class LogWriterFactoryTest {

  @Rule
  public TestName testName = new TestName();

  @Test
  public void createLogWriterLoggerReturnsLogWriter() {
    LogWriterLogger logWriterLogger =
        LogWriterFactory.createLogWriterLogger(getClass().getName(), testName.getMethodName(),
            false);

    assertThat(logWriterLogger).isInstanceOf(LogWriter.class);
  }

  @Test
  public void createLogWriterLoggerReturnsLoggerWithSpecifiedName() {
    LogWriterLogger logWriterLogger =
        LogWriterFactory.createLogWriterLogger(getClass().getName(), testName.getMethodName(),
            false);

    assertThat(logWriterLogger.getName()).isEqualTo(getClass().getName());
  }

  @Test
  public void createLogWriterLoggerReturnsLoggerWithSpecifiedConnectionName() {
    LogWriterLogger logWriterLogger =
        LogWriterFactory.createLogWriterLogger(getClass().getName(), testName.getMethodName(),
            false);

    assertThat(logWriterLogger.getConnectionName()).isEqualTo(testName.getMethodName());
  }

  @Test
  public void createLogWriterLoggerWithSecureFalseReturnsSecureLogWriter() {
    LogWriterLogger logWriterLogger =
        LogWriterFactory.createLogWriterLogger(getClass().getName(), testName.getMethodName(),
            false);

    assertThat(logWriterLogger.isSecure()).isFalse();
  }

  @Test
  public void createLogWriterLoggerWithSecureTrueReturnsSecureLogWriter() {
    LogWriterLogger logWriterLogger =
        LogWriterFactory.createLogWriterLogger(getClass().getName(), testName.getMethodName(),
            true);

    assertThat(logWriterLogger.isSecure()).isTrue();
  }
}
