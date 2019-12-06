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

import static org.apache.geode.management.internal.cli.util.LogFilter.LineFilterResult.LINE_ACCEPTED;
import static org.apache.geode.management.internal.cli.util.LogFilter.LineFilterResult.LINE_REJECTED;
import static org.apache.geode.management.internal.cli.util.LogFilter.LineFilterResult.REMAINDER_OF_FILE_REJECTED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.spi.FileSystemProvider;
import java.time.LocalDateTime;

import org.apache.logging.log4j.Level;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.categories.LoggingTest;

@Category({GfshTest.class, LoggingTest.class})
public class LogFilterTest {

  @Test
  public void permittedLogLevelsCanFilterLines() {
    LogFilter logFilter = new LogFilter(Level.INFO, null, null);

    LocalDateTime now = LocalDateTime.now();
    assertThat(logFilter.acceptsLogEntry(Level.INFO, now)).isEqualTo(LINE_ACCEPTED);
    assertThat(logFilter.acceptsLogEntry(Level.WARN, now)).isEqualTo(LINE_ACCEPTED);
    assertThat(logFilter.acceptsLogEntry(Level.DEBUG, now)).isEqualTo(LINE_REJECTED);
    assertThat(logFilter.acceptsLogEntry(Level.TRACE, now)).isEqualTo(LINE_REJECTED);
  }

  @Test
  public void permittedOnlyLogLevels() {
    LogFilter logFilter = new LogFilter(Level.INFO, true, null, null);

    LocalDateTime now = LocalDateTime.now();
    assertThat(logFilter.acceptsLogEntry(Level.INFO, now)).isEqualTo(LINE_ACCEPTED);
    assertThat(logFilter.acceptsLogEntry(Level.WARN, now)).isEqualTo(LINE_REJECTED);
    assertThat(logFilter.acceptsLogEntry(Level.DEBUG, now)).isEqualTo(LINE_REJECTED);
    assertThat(logFilter.acceptsLogEntry(Level.TRACE, now)).isEqualTo(LINE_REJECTED);
  }

  @Test
  public void permittedLogLevelsALL() {
    LogFilter logFilter = new LogFilter(Level.ALL, null, null);

    LocalDateTime now = LocalDateTime.now();
    assertThat(logFilter.acceptsLogEntry(Level.INFO, now)).isEqualTo(LINE_ACCEPTED);
    assertThat(logFilter.acceptsLogEntry(Level.WARN, now)).isEqualTo(LINE_ACCEPTED);
    assertThat(logFilter.acceptsLogEntry(Level.DEBUG, now)).isEqualTo(LINE_ACCEPTED);
    assertThat(logFilter.acceptsLogEntry(Level.TRACE, now)).isEqualTo(LINE_ACCEPTED);
  }

  @Test
  public void startDateCanFilterLines() {
    LocalDateTime startDate = LocalDateTime.now().minusDays(2);

    LogFilter logFilter = new LogFilter(Level.ALL, startDate, null);

    assertThat(logFilter.acceptsLogEntry(Level.INFO, LocalDateTime.now())).isEqualTo(LINE_ACCEPTED);
    assertThat(logFilter.acceptsLogEntry(Level.INFO, startDate)).isEqualTo(LINE_ACCEPTED);
    assertThat(logFilter.acceptsLogEntry(Level.DEBUG, startDate)).isEqualTo(LINE_ACCEPTED);

    assertThat(logFilter.acceptsLogEntry(Level.INFO, LocalDateTime.now().minusDays(3)))
        .isEqualTo(LINE_REJECTED);
  }

  @Test
  public void endDateCanFilterLines() {
    LocalDateTime endDate = LocalDateTime.now().minusDays(2);

    LogFilter logFilter = new LogFilter(Level.ALL, null, endDate);

    assertThat(logFilter.acceptsLogEntry(Level.INFO, LocalDateTime.now().minusDays(3)))
        .isEqualTo(LINE_ACCEPTED);

    assertThat(logFilter.acceptsLogEntry(Level.INFO, endDate)).isEqualTo(LINE_ACCEPTED);
    assertThat(logFilter.acceptsLogEntry(Level.DEBUG, endDate)).isEqualTo(LINE_ACCEPTED);

    assertThat(logFilter.acceptsLogEntry(Level.INFO, LocalDateTime.now()))
        .isEqualTo(REMAINDER_OF_FILE_REJECTED);
  }

  @Test
  public void filterWorksWithLevelBasedAndTimeBasedFiltering() {
    LocalDateTime startDate = LocalDateTime.now().minusDays(5);
    LocalDateTime endDate = LocalDateTime.now().minusDays(2);

    LogFilter logFilter = new LogFilter(Level.INFO, startDate, endDate);

    assertThat(logFilter.acceptsLogEntry(Level.ERROR, LocalDateTime.now().minusDays(6)))
        .isEqualTo(LINE_REJECTED);
    assertThat(logFilter.acceptsLogEntry(Level.INFO, LocalDateTime.now().minusDays(6)))
        .isEqualTo(LINE_REJECTED);

    assertThat(logFilter.acceptsLogEntry(null)).isEqualTo(LINE_REJECTED);

    assertThat(logFilter.acceptsLogEntry(Level.ERROR, LocalDateTime.now().minusDays(6)))
        .isEqualTo(LINE_REJECTED);
    assertThat(logFilter.acceptsLogEntry(null)).isEqualTo(LINE_REJECTED);

    assertThat(logFilter.acceptsLogEntry(Level.INFO, LocalDateTime.now().minusDays(4)))
        .isEqualTo(LINE_ACCEPTED);
    assertThat(logFilter.acceptsLogEntry(null)).isEqualTo(LINE_ACCEPTED);

    assertThat(logFilter.acceptsLogEntry(Level.ERROR, LocalDateTime.now().minusDays(1)))
        .isEqualTo(REMAINDER_OF_FILE_REJECTED);
    assertThat(logFilter.acceptsLogEntry(null)).isEqualTo(REMAINDER_OF_FILE_REJECTED);

    assertThat(logFilter.acceptsLogEntry(Level.INFO, LocalDateTime.now().minusDays(1)))
        .isEqualTo(REMAINDER_OF_FILE_REJECTED);
  }

  @Test
  public void firstLinesAreAcceptedIfParsableLineHasNotBeenSeenYet() {
    LogFilter logFilter = new LogFilter(Level.INFO, null, null);

    assertThat(logFilter.acceptsLogEntry(null)).isEqualTo(LINE_ACCEPTED);
    assertThat(logFilter.acceptsLogEntry(null)).isEqualTo(LINE_ACCEPTED);

    assertThat(logFilter.acceptsLogEntry(Level.DEBUG, LocalDateTime.now()))
        .isEqualTo(LINE_REJECTED);
    assertThat(logFilter.acceptsLogEntry(null)).isEqualTo(LINE_REJECTED);
  }

  @Test
  public void testAcceptFileWithCreateTimeNotAvailale() {
    Path path = mock(Path.class);
    when(path.toFile()).thenReturn(mock(File.class));
    when(path.toFile().lastModified()).thenReturn(System.currentTimeMillis());
    when(path.getFileSystem()).thenThrow(SecurityException.class);

    // a filter with no start/end date should accept this file
    LogFilter filter = new LogFilter(Level.INFO, null, null);
    assertThat(filter.acceptsFile(path)).isTrue();

    // a filter with a start date of now should not accept the file
    filter = new LogFilter(Level.INFO, LocalDateTime.now(), null);
    assertThat(filter.acceptsFile(path)).isFalse();

    // a filter with a start date of now minus an hour should not accept the file
    filter = new LogFilter(Level.INFO, LocalDateTime.now().minusHours(1), null);
    assertThat(filter.acceptsFile(path)).isTrue();

    // a filter with an end date of now should accept the file
    filter = new LogFilter(Level.INFO, null, LocalDateTime.now());
    assertThat(filter.acceptsFile(path)).isTrue();

    // a filter with an end date of an hour ago should also accept the file, because we only
    // know the last modified time of the file, when don't know what time this file is created, it
    // may still be created more than an hour ago.
    filter = new LogFilter(Level.INFO, null, LocalDateTime.now().minusHours(1));
    assertThat(filter.acceptsFile(path)).isTrue();
  }

  @Test
  public void testAcceptFileWithCreateTimeAvailable() throws Exception {
    Path path = mock(Path.class);
    when(path.toFile()).thenReturn(mock(File.class));
    when(path.toFile().lastModified()).thenReturn(System.currentTimeMillis());
    BasicFileAttributes attributes = mock(BasicFileAttributes.class);
    when(path.getFileSystem()).thenReturn(mock(FileSystem.class));
    when(path.getFileSystem().provider()).thenReturn(mock(FileSystemProvider.class));
    when(path.getFileSystem().provider().readAttributes(path, BasicFileAttributes.class))
        .thenReturn(attributes);

    // a filter with no start/end date should accept this file
    LogFilter filter = new LogFilter(Level.INFO, null, null);
    assertThat(filter.acceptsFile(path)).isTrue();

    // a filter with a start date of now should not accept the file
    filter = new LogFilter(Level.INFO, LocalDateTime.now(), null);
    assertThat(filter.acceptsFile(path)).isFalse();

    // a filter with a start date of now minus an hour should not accept the file
    filter = new LogFilter(Level.INFO, LocalDateTime.now().minusHours(1), null);
    assertThat(filter.acceptsFile(path)).isTrue();

    // a filter with an end date of now should accept the file
    filter = new LogFilter(Level.INFO, null, LocalDateTime.now());
    assertThat(filter.acceptsFile(path)).isTrue();

    // a filter with an end date of an hour ago should accept the file
    filter = new LogFilter(Level.INFO, null, LocalDateTime.now().minusHours(1));
    assertThat(filter.acceptsFile(path)).isTrue();
  }
}
