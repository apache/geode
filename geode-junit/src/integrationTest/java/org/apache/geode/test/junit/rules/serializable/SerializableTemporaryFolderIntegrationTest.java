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
package org.apache.geode.test.junit.rules.serializable;

import static org.apache.geode.test.junit.runners.TestRunner.runTestWithValidation;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

public class SerializableTemporaryFolderIntegrationTest {

  private static final AtomicReference<File> ROOT = new AtomicReference<>();
  private static final AtomicReference<File> DESTINATION = new AtomicReference<>();
  private static final AtomicReference<String> TEST_NAME = new AtomicReference<>();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void delete_false_doesNotDeleteTemporaryFolder() {
    runTestWithValidation(DeleteFalse.class);

    File root = ROOT.get();

    assertThat(root).exists();
  }

  @Test
  public void delete_false_deletesTemporaryFolder() {
    runTestWithValidation(DeleteTrue.class);

    File root = ROOT.get();

    assertThat(root).doesNotExist();
  }

  @Test
  public void copyTo_savesTemporaryFolderToDestination() {
    DESTINATION.set(temporaryFolder.getRoot());

    runTestWithValidation(CopyTo.class);

    File destinationFolder = DESTINATION.get();
    assertThat(destinationFolder).exists();

    File testFolder = new File(destinationFolder, TEST_NAME.get());
    assertThat(testFolder).exists();

    File subFolder = new File(testFolder, CopyTo.FOLDER_NAME);
    assertThat(subFolder).exists();

    File file = new File(subFolder, CopyTo.FILE_NAME);
    assertThat(file).exists();
    assertThat(file).hasContent(CopyTo.DATA);
  }

  public static class DeleteFalse {

    @Rule
    public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder()
        .delete(false);

    @Test
    public void captureRoot() {
      ROOT.set(temporaryFolder.getRoot());
    }
  }

  public static class DeleteTrue {

    @Rule
    public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder()
        .delete(true);

    @Test
    public void captureRoot() {
      ROOT.set(temporaryFolder.getRoot());
    }
  }

  public static class CopyTo {

    private static final String FOLDER_NAME = "myFolder";
    private static final String FILE_NAME = "myFile.txt";
    private static final String DATA = "data";

    @Rule
    public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder()
        .copyTo(DESTINATION.get());
    @Rule
    public TestName testName = new TestName();

    @Before
    public void preCondition() {
      assertThat(DESTINATION.get()).exists();
    }

    @Test
    public void writeToTemporaryFolder() throws Exception {
      TEST_NAME.set(testName.getMethodName());
      File folder = temporaryFolder.newFolder(FOLDER_NAME);
      File file = new File(folder, FILE_NAME);
      assertThat(file.createNewFile()).isTrue();
      FileUtils.writeStringToFile(file, DATA, Charset.defaultCharset());
    }
  }
}
