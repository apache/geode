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

package org.apache.geode.management.internal.cli.result;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.management.internal.cli.result.model.FileResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;

public class FileResultTest {

  private ResultModel fileResult;

  @Before
  public void setUp() {
    fileResult = new ResultModel();
  }

  @Test
  public void getFormattedFileListReturnsCommaDelimitedStringOfFiles() {
    fileResult.addFile(new File("file1.txt"), FileResultModel.FILE_TYPE_FILE);
    fileResult.addFile(new File("file2.txt"), FileResultModel.FILE_TYPE_FILE);

    assertThat(fileResult.getFormattedFileList()).isEqualTo("file1.txt, file2.txt");
  }

  @Test
  public void getFileListReturnsListOfFilesInAnyOrder() {
    File file1 = new File("file1.txt");
    File file2 = new File("file2.txt");
    fileResult.addFile(file1, FileResultModel.FILE_TYPE_FILE);
    fileResult.addFile(file2, FileResultModel.FILE_TYPE_FILE);

    assertThat(fileResult.getFileList()).containsExactlyInAnyOrder(file1, file2);
  }
}
