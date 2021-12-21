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

package org.apache.geode.test.junit.rules;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;

import com.google.common.io.Files;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.Result;

import org.apache.geode.test.junit.runners.TestRunner;

public class TemporaryFileRuleTest {

  @Test
  public void exceptionIsThrownIfFileAlreadyExists() {
    Result result =
        TestRunner.runTest(TemporaryFileRuleTest.ExceptionIsThrownIfFileAlreadyExists.class);

    assertThat(result.wasSuccessful()).isTrue();
  }

  @Test
  public void fileGetsCreatedProperly() {
    Result result = TestRunner.runTest(TemporaryFileRuleTest.FileGetsCreatedProperly.class);

    assertThat(result.wasSuccessful()).isTrue();
  }


  @Test
  public void filesGetCleanedUpAfterTestMethod() {
    Result result =
        TestRunner.runTest(TemporaryFileRuleTest.FilesGetCleanedUpAfterTestMethod.class);

    assertThat(result.wasSuccessful()).isTrue();
  }

  /**
   * Used by test {@link #exceptionIsThrownIfFileAlreadyExists()}
   */
  public static class ExceptionIsThrownIfFileAlreadyExists {

    static File tempDirectory = Files.createTempDir();

    @Rule
    public TemporaryFileRule temporaryFileRule =
        TemporaryFileRule.inDirectory(tempDirectory.getAbsolutePath());

    @Test
    public void doTest() throws Exception {
      String fileName = "fileThatAlreadyExists.txt";
      File tempFile = new File(tempDirectory, fileName);
      assertThat(tempFile.createNewFile()).isTrue();
      assertThatThrownBy(() -> temporaryFileRule.newFile(fileName))
          .isInstanceOf(IllegalStateException.class);
    }
  }


  /**
   * Used by test {@link #fileGetsCreatedProperly()}
   */
  public static class FileGetsCreatedProperly {

    static File tempDirectory = Files.createTempDir();

    @Rule
    public TemporaryFileRule temporaryFileRule =
        TemporaryFileRule.inDirectory(tempDirectory.getAbsolutePath());

    @Test
    public void doTest() throws Exception {
      String fileName = "expectedFile.txt";
      File expectedFile = new File(tempDirectory, fileName);
      File actualFile = temporaryFileRule.newFile(fileName);

      assertThat(actualFile).isEqualTo(expectedFile);
    }
  }

  /**
   * Used by test {@link #filesGetCleanedUpAfterTestMethod()}
   *
   * This test ensures that {@link TemporaryFileRule} cleans up the files it created in between each
   * test method.
   */
  public static class FilesGetCleanedUpAfterTestMethod {

    private static final String fileName1 = "test1.txt";
    private static final String fileName2 = "test2.txt";

    static File tempDirectory = Files.createTempDir();

    @Rule
    public TemporaryFileRule temporaryFileRule =
        TemporaryFileRule.inDirectory(tempDirectory.getAbsolutePath());

    @Test
    public void test1() throws Exception {
      temporaryFileRule.newFile(fileName1);

      assertThat(new File(tempDirectory, fileName1)).exists();
      assertThat(new File(tempDirectory, fileName2)).doesNotExist();
    }

    @Test
    public void test2() throws Exception {
      temporaryFileRule.newFile(fileName2);

      assertThat(new File(tempDirectory, fileName1)).doesNotExist();
      assertThat(new File(tempDirectory, fileName2)).exists();
    }
  }

}
