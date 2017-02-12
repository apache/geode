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
 *
 */

package org.apache.geode.management.internal.cli.util;

import static java.util.stream.Collectors.toSet;
import static org.apache.geode.management.internal.cli.util.LogFilter.LineFilterResult.LINE_ACCEPTED;
import static org.apache.geode.management.internal.cli.util.LogFilter.LineFilterResult.LINE_REJECTED;
import static org.apache.geode.management.internal.cli.util.LogFilter.LineFilterResult.REMAINDER_OF_FILE_REJECTED;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.time.LocalDateTime;
import java.util.Set;
import java.util.stream.Stream;

@Category(UnitTest.class)
public class LogFilterTest {
  @Test
  public void permittedLogLevelsCanFilterLines() throws Exception {
    Set<String> permittedLogLevels = Stream.of("info", "finest").collect(toSet());

    LogFilter logFilter = new LogFilter(permittedLogLevels, null, null);

    LocalDateTime now = LocalDateTime.now();
    assertThat(logFilter.acceptsLogEntry("info", now)).isEqualTo(LINE_ACCEPTED);
    assertThat(logFilter.acceptsLogEntry("finest", now)).isEqualTo(LINE_ACCEPTED);
    assertThat(logFilter.acceptsLogEntry("fine", now)).isEqualTo(LINE_REJECTED);
    assertThat(logFilter.acceptsLogEntry("error", now)).isEqualTo(LINE_REJECTED);
  }


  @Test
  public void startDateCanFilterLines() {
    LocalDateTime startDate = LocalDateTime.now().minusDays(2);

    LogFilter logFilter = new LogFilter(null, startDate, null);

    assertThat(logFilter.acceptsLogEntry("info", LocalDateTime.now())).isEqualTo(LINE_ACCEPTED);
    assertThat(logFilter.acceptsLogEntry("info", startDate)).isEqualTo(LINE_ACCEPTED);
    assertThat(logFilter.acceptsLogEntry("fine", startDate)).isEqualTo(LINE_ACCEPTED);

    assertThat(logFilter.acceptsLogEntry("info", LocalDateTime.now().minusDays(3)))
        .isEqualTo(LINE_REJECTED);
  }

  @Test
  public void endDateCanFilterLines() {
    LocalDateTime endDate = LocalDateTime.now().minusDays(2);

    LogFilter logFilter = new LogFilter(null, null, endDate);

    assertThat(logFilter.acceptsLogEntry("info", LocalDateTime.now().minusDays(3)))
        .isEqualTo(LINE_ACCEPTED);

    assertThat(logFilter.acceptsLogEntry("info", endDate)).isEqualTo(LINE_ACCEPTED);
    assertThat(logFilter.acceptsLogEntry("fine", endDate)).isEqualTo(LINE_ACCEPTED);

    assertThat(logFilter.acceptsLogEntry("info", LocalDateTime.now()))
        .isEqualTo(REMAINDER_OF_FILE_REJECTED);
  }

  @Test
  public void filterWorksWithLevelBasedAndTimeBasedFiltering() {
    LocalDateTime startDate = LocalDateTime.now().minusDays(5);
    LocalDateTime endDate = LocalDateTime.now().minusDays(2);

    Set<String> permittedLogLevels = Stream.of("info", "finest").collect(toSet());

    LogFilter logFilter = new LogFilter(permittedLogLevels, startDate, endDate);


    assertThat(logFilter.acceptsLogEntry("error", LocalDateTime.now().minusDays(6)))
        .isEqualTo(LINE_REJECTED);
    assertThat(logFilter.acceptsLogEntry("info", LocalDateTime.now().minusDays(6)))
        .isEqualTo(LINE_REJECTED);

    assertThat(logFilter.acceptsLogEntry(null)).isEqualTo(LINE_REJECTED);


    assertThat(logFilter.acceptsLogEntry("error", LocalDateTime.now().minusDays(4)))
        .isEqualTo(LINE_REJECTED);
    assertThat(logFilter.acceptsLogEntry(null)).isEqualTo(LINE_REJECTED);

    assertThat(logFilter.acceptsLogEntry("info", LocalDateTime.now().minusDays(4)))
        .isEqualTo(LINE_ACCEPTED);
    assertThat(logFilter.acceptsLogEntry(null)).isEqualTo(LINE_ACCEPTED);



    assertThat(logFilter.acceptsLogEntry("error", LocalDateTime.now().minusDays(1)))
        .isEqualTo(REMAINDER_OF_FILE_REJECTED);
    assertThat(logFilter.acceptsLogEntry(null)).isEqualTo(REMAINDER_OF_FILE_REJECTED);

    assertThat(logFilter.acceptsLogEntry("info", LocalDateTime.now().minusDays(1)))
        .isEqualTo(REMAINDER_OF_FILE_REJECTED);
  }

  @Test
  public void firstLinesAreAcceptedIfParsableLineHasNotBeenSeenYet() {
    LogFilter logFilter = new LogFilter(Stream.of("info").collect(toSet()), null, null);

    assertThat(logFilter.acceptsLogEntry(null)).isEqualTo(LINE_ACCEPTED);
    assertThat(logFilter.acceptsLogEntry(null)).isEqualTo(LINE_ACCEPTED);

    assertThat(logFilter.acceptsLogEntry("error", LocalDateTime.now())).isEqualTo(LINE_REJECTED);
    assertThat(logFilter.acceptsLogEntry(null)).isEqualTo(LINE_REJECTED);

  }
}
