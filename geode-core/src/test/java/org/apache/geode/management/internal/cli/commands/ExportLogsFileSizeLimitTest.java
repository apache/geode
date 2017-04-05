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
package org.apache.geode.management.internal.cli.commands;

import static org.apache.geode.management.internal.cli.commands.ExportLogsCommand.MEGABYTE;
import static org.assertj.core.api.Assertions.*;

import org.apache.commons.io.FileUtils;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

@Category(IntegrationTest.class)
public class ExportLogsFileSizeLimitTest {

  private File dir;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  @Before
  public void before() throws Exception {
    this.dir = this.temporaryFolder.getRoot();
  }

  @Test
  public void sizeOverLimitThrows() throws Exception {
    File file = new File(this.dir, this.testName.getMethodName());
    fillUpFile(file, MEGABYTE * 2);

    ExportLogsCommand exportLogsCommand = new ExportLogsCommand();

    assertThatThrownBy(
        () -> exportLogsCommand.isFileSizeCheckEnabledAndWithinLimit(MEGABYTE, file));
  }

  @Test
  public void sizeLessThanLimitIsOk() throws Exception {
    File file = new File(this.dir, this.testName.getMethodName());
    fillUpFile(file, MEGABYTE / 2);

    ExportLogsCommand exportLogsCommand = new ExportLogsCommand();

    assertThat(exportLogsCommand.isFileSizeCheckEnabledAndWithinLimit(MEGABYTE, file)).isTrue();
  }

  @Test
  public void sizeZeroIsUnlimitedSize() throws Exception {
    File file = new File(this.dir, this.testName.getMethodName());
    fillUpFile(file, MEGABYTE * 2);

    ExportLogsCommand exportLogsCommand = new ExportLogsCommand();

    assertThat(exportLogsCommand.isFileSizeCheckEnabledAndWithinLimit(0, file)).isFalse();
  }

  private void fillUpFile(File file, int sizeInBytes) throws IOException {
    PrintWriter writer = new PrintWriter(file, "UTF-8");
    while (FileUtils.sizeOf(file) < sizeInBytes) {
      writer.println("this is a line of data in the file");
    }
    writer.close();
  }

}
