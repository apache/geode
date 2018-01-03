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
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.UnitTest;


@Category(UnitTest.class)
public class FileResultTest {

  private FileResult fileResult;

  @Before
  public void before() {
    fileResult = new FileResult();
  }

  @Test
  public void getFormattedFileList() {
    fileResult.addFile(new File("file1.txt"));
    fileResult.addFile(new File("file2.txt"));
    assertThat(fileResult.getFormattedFileList()).isEqualTo("file1.txt, file2.txt");
  }

  @Test
  public void getFiles() {
    assertThat(fileResult.getFiles()).isEmpty();

    File file1 = new File("file1.txt");
    File file2 = new File("file2.txt");
    fileResult.addFile(file1);
    fileResult.addFile(file2);
    assertThat(fileResult.getFiles()).containsExactlyInAnyOrder(file1, file2);
  }
}
