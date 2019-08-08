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
package org.apache.geode.logging.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Integration tests to confirm that writing to file system works properly in stressTest
 */
@Category(LoggingTest.class)
public class FileSystemCanaryIntegrationTest {

  private File file;
  private Collection<String> lines;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() {
    String name = testName.getMethodName();
    file = new File(temporaryFolder.getRoot(), name + "-main.log");

    lines = new ArrayList<>();
    for (int i = 1; i <= 10; i++) {
      lines.add("Line-" + i);
    }
  }

  @Test
  public void readLinesReturnsSameLinesThatWereJustWritten() throws IOException {
    FileUtils.writeLines(file, lines);

    assertThat(FileUtils.readLines(file, Charset.defaultCharset())).isEqualTo(lines);
  }

}
