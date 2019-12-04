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

package org.apache.geode.management.internal.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


public class ManagementUtilsTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void filesToBytesAndThenBytesToFiles() throws IOException {
    File file1 = new File(temporaryFolder.getRoot(), "file1.txt");
    File file2 = new File(temporaryFolder.getRoot(), "file2.txt");

    FileUtils.write(file1, "file1-content", "UTF-8");
    FileUtils.write(file2, "file2-content", "UTF-8");

    List<String> fileNames = Arrays.asList(file1.getAbsolutePath(), file2.getAbsolutePath());
    Byte[][] bytes = ManagementUtils.filesToBytes(fileNames);

    File dir = temporaryFolder.newFolder("temp");
    List<String> filePaths = ManagementUtils.bytesToFiles(bytes, dir.getAbsolutePath());

    assertThat(filePaths).hasSize(2);
    assertThat(new File(filePaths.get(0))).hasContent("file1-content");
    assertThat(new File(filePaths.get(1))).hasContent("file2-content");
  }
}
