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

package org.apache.geode.management.internal.cli.result.model;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.management.internal.cli.result.ModelCommandResult;
import org.apache.geode.test.junit.assertions.ResultModelAssert;

public class ResultModelIntegrationTest {

  private ResultModel result;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    result = new ResultModel();
    result.addFile("test1.txt", "hello");
    result.addFile("test2.txt", "hello again");
  }

  @Test
  public void emptyFileSizeDoesNothing() throws IOException {
    ResultModel emptyFileResult = new ResultModel();
    result.saveFileTo(temporaryFolder.newFolder());
    assertThat(emptyFileResult.getInfoSections()).hasSize(0);
  }

  @Test
  public void savesToNoWhereDoesNothing() throws IOException {
    result.saveFileTo(null);
    assertThat(result.getInfoSections()).hasSize(0);
  }

  @Test
  public void notADirectory() throws IOException {
    result.saveFileTo(temporaryFolder.newFile());
    assertThis(result).hasInfoResultModel("fileSave").hasOutput().contains("is not a directory");
  }

  @Test
  public void dirNotExistBefore() throws IOException {
    File dir = temporaryFolder.newFolder("test");
    dir.delete();

    result.saveFileTo(dir);
    assertThat(dir).exists();
    File file1 = new File(dir, "test1.txt");
    File file2 = new File(dir, "test2.txt");
    assertThat(dir.listFiles()).contains(file1, file2);

    assertThis(result).hasInfoResultModel("fileSave").hasLines().hasSize(2)
        .containsExactlyInAnyOrder(
            "File saved to " + file1.getAbsolutePath(),
            "File saved to " + file2.getAbsolutePath());
  }

  @Test
  public void modelCommandResultShouldNotDealWithFiles() throws IOException {
    result.saveFileTo(temporaryFolder.newFolder("test"));
    ModelCommandResult commandResult = new ModelCommandResult(result);
    assertThat(commandResult.hasIncomingFiles()).isFalse();
    assertThat(commandResult.getNumTimesSaved()).isEqualTo(0);
  }

  public static ResultModelAssert assertThis(ResultModel model) {
    return new ResultModelAssert(model);
  }
}
