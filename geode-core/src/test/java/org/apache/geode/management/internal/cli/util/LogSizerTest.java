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

import static java.io.File.separator;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.geode.management.internal.cli.functions.ExportLogsFunction;
import org.apache.geode.management.internal.cli.functions.SizeExportLogsFunction;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.text.ParseException;

@Category(UnitTest.class)
public class LogSizerTest {
  private LogFilter logFilter;
  private SizeExportLogsFunction.Args nonFilteringArgs;

  private File dir;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() throws Exception {
    logFilter = mock(LogFilter.class);
    dir = temporaryFolder.getRoot();
    nonFilteringArgs = new ExportLogsFunction.Args(null, null, null, false, false, false);

  }

  @After
  public void tearDown() throws Exception {

  }

  @Test
  public void nullFileArgs_returnsZeroSize() throws ParseException, IOException {
    LogSizer sizer = new LogSizer(logFilter, null, null);
    assertThat(sizer.getFilteredSize()).isEqualTo(0L);
  }

  @Test
  public void noFiles_returnsZeroSize() throws ParseException, IOException {

    File mockStatFile = mock(File.class);
    File mockLogFile = mock(File.class);
    when(mockLogFile.toPath()).thenReturn(
        new File("root" + separator + "parent" + separator + testName + ".log").toPath());
    when(mockStatFile.toPath()).thenReturn(
        new File("root" + separator + "parent" + separator + testName + ".gfs").toPath());
    LogSizer sizer = new LogSizer(logFilter, mockLogFile, mockStatFile);
    assertThat(sizer.getFilteredSize()).isEqualTo(0L);
  }

  @Test
  public void emptyFiles_returnsZeroSize() throws ParseException, IOException {

    File mockStatFile = mock(File.class);
    File mockLogFile = mock(File.class);
    when(mockLogFile.toPath()).thenReturn(
        new File("root" + separator + "parent" + separator + testName + ".log").toPath());
    when(mockStatFile.toPath()).thenReturn(
        new File("root" + separator + "parent" + separator + testName + ".gfs").toPath());
    LogFilter logFilter =
        new LogFilter(nonFilteringArgs.getLogLevel(), nonFilteringArgs.isThisLogLevelOnly(),
            nonFilteringArgs.getStartTime(), nonFilteringArgs.getEndTime());

    LogSizer sizer = new LogSizer(logFilter, mockLogFile, mockStatFile);
    assertThat(sizer.getFilteredSize()).isEqualTo(0L);
  }

}
