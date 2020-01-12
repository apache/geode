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
package org.apache.geode.management.internal.cli.util;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.logging.log4j.Level;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.categories.LoggingTest;

@Category({GfshTest.class, LoggingTest.class})
public class LogLevelExtractorTest {

  @Test
  public void extractWorksCorrectlyForLineFromLogFile() {
    String logLine =
        "[info 2017/02/07 11:16:36.694 PST locator1 <locator request thread[1]> tid=0x27] Mapped \"{[/v1/async-event-queues],methods=[GET]}\" onto public java.lang.String";

    LogLevelExtractor.Result result = LogLevelExtractor.extract(logLine);

    assertThat(result).isNotNull();
    assertThat(result.getLogLevel()).isEqualTo(Level.INFO);

    assertThat(result.getLogTimestamp().toString()).isEqualTo("2017-02-07T11:16:36.694");
  }

  @Test
  public void extractWorksForFine() {
    String logLine =
        "[fine 2017/02/07 11:16:36.694 PST locator1 <locator request thread[1]> tid=0x27] Mapped \"{[/v1/async-event-queues],methods=[GET]}\" onto public java.lang.String";

    LogLevelExtractor.Result result = LogLevelExtractor.extract(logLine);

    assertThat(result).isNotNull();
    assertThat(result.getLogLevel()).isEqualTo(Level.DEBUG);

    assertThat(result.getLogTimestamp().toString()).isEqualTo("2017-02-07T11:16:36.694");
  }

  @Test
  public void extractWorksForFiner() {
    String logLine =
        "[finer 2017/02/07 11:16:36.694 PST locator1 <locator request thread[1]> tid=0x27] Mapped \"{[/v1/async-event-queues],methods=[GET]}\" onto public java.lang.String";

    LogLevelExtractor.Result result = LogLevelExtractor.extract(logLine);

    assertThat(result).isNotNull();
    assertThat(result.getLogLevel()).isEqualTo(Level.TRACE);
    assertThat(result.getLogTimestamp().toString()).isEqualTo("2017-02-07T11:16:36.694");
  }

  @Test
  public void extractWorksForFinest() {
    String logLine =
        "[finest 2017/02/07 11:16:36.694 PST locator1 <locator request thread[1]> tid=0x27] Mapped \"{[/v1/async-event-queues],methods=[GET]}\" onto public java.lang.String";

    LogLevelExtractor.Result result = LogLevelExtractor.extract(logLine);

    assertThat(result).isNotNull();
    assertThat(result.getLogLevel()).isEqualTo(Level.TRACE);
    assertThat(result.getLogTimestamp().toString()).isEqualTo("2017-02-07T11:16:36.694");
  }

  @Test
  public void extractReturnsNullIfNoTimestamp() {
    String logLine = "[info (this line is not a valid log statement since it has no timestamp)";

    LogLevelExtractor.Result result = LogLevelExtractor.extract(logLine);

    assertThat(result).isNull();
  }

  @Test
  public void extractReturnsNullIfLineDoesNotMatchPattern() {
    String logLine = "some line containing a date like 2017/02/07 11:16:36.694 PST ";

    LogLevelExtractor.Result result = LogLevelExtractor.extract(logLine);

    assertThat(result).isNull();
  }
}
