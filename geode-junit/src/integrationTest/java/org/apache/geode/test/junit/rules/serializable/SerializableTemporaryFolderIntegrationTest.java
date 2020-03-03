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

import static org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder.When.ALWAYS;
import static org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder.When.FAILS;
import static org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder.When.PASSES;
import static org.apache.geode.test.junit.runners.TestRunner.runTestWithExpectedFailureTypes;
import static org.apache.geode.test.junit.runners.TestRunner.runTestWithValidation;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

public class SerializableTemporaryFolderIntegrationTest {

  private static final Pattern DIGITS_REGEX = Pattern.compile("\\d+");

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
  public void copyTo_whenAlways_copiesFolderToDestination_ifTestPasses() {
    DESTINATION.set(temporaryFolder.getRoot());

    runTestWithValidation(CopyTo_WhenAlways_Passes.class);

    File destinationFolder = DESTINATION.get();
    assertThat(destinationFolder).exists();

    File timestampFolder = Arrays.stream(destinationFolder.listFiles()).findFirst().get();
    assertThat(timestampFolder.getName()).matches(DIGITS_REGEX);

    File testFolder = new File(timestampFolder, TEST_NAME.get());
    assertThat(testFolder).exists();

    File subFolder = new File(testFolder, CopyTo_Passes.FOLDER_NAME);
    assertThat(subFolder).exists();

    File file = new File(subFolder, CopyTo_Passes.FILE_NAME);
    assertThat(file).exists().hasContent(CopyTo_Passes.DATA);
  }

  @Test
  public void copyTo_whenAlways_copiesFolderToDestination_ifTestFails() {
    DESTINATION.set(temporaryFolder.getRoot());

    runTestWithExpectedFailureTypes(CopyTo_WhenAlways_Fails.class, ExpectedError.class);

    File destinationFolder = DESTINATION.get();
    assertThat(destinationFolder).exists();

    File timestampFolder = Arrays.stream(destinationFolder.listFiles()).findFirst().get();
    assertThat(timestampFolder.getName()).matches(DIGITS_REGEX);

    File testFolder = new File(timestampFolder, TEST_NAME.get());
    assertThat(testFolder).exists();

    File subFolder = new File(testFolder, CopyTo_Passes.FOLDER_NAME);
    assertThat(subFolder).exists();

    File file = new File(subFolder, CopyTo_Passes.FILE_NAME);
    assertThat(file).exists().hasContent(CopyTo_Passes.DATA);
  }

  @Test
  public void copyTo_whenFails_doesNotCopyFolder_ifTestPasses() {
    DESTINATION.set(temporaryFolder.getRoot());

    runTestWithValidation(CopyTo_WhenFails_Passes.class);

    File destinationFolder = DESTINATION.get();
    assertThat(destinationFolder).exists();

    assertThat(destinationFolder.listFiles()).isEmpty();
  }

  @Test
  public void copyTo_whenFails_copiesFolderToDestination_ifTestFails() {
    DESTINATION.set(temporaryFolder.getRoot());

    runTestWithExpectedFailureTypes(CopyTo_WhenFails_Fails.class, ExpectedError.class);

    File destinationFolder = DESTINATION.get();
    assertThat(destinationFolder).exists();

    File timestampFolder = Arrays.stream(destinationFolder.listFiles()).findFirst().get();
    assertThat(timestampFolder.getName()).matches(DIGITS_REGEX);

    File testFolder = new File(timestampFolder, TEST_NAME.get());
    assertThat(testFolder).exists();

    File subFolder = new File(testFolder, CopyTo_Passes.FOLDER_NAME);
    assertThat(subFolder).exists();

    File file = new File(subFolder, CopyTo_Passes.FILE_NAME);
    assertThat(file).exists().hasContent(CopyTo_Passes.DATA);
  }

  @Test
  public void copyTo_whenPasses_copiesFolderToDestination_ifTestPasses() {
    DESTINATION.set(temporaryFolder.getRoot());

    runTestWithValidation(CopyTo_WhenPasses_Passes.class);

    File destinationFolder = DESTINATION.get();
    assertThat(destinationFolder).exists();

    File timestampFolder = Arrays.stream(destinationFolder.listFiles()).findFirst().get();
    assertThat(timestampFolder.getName()).matches(DIGITS_REGEX);

    File testFolder = new File(timestampFolder, TEST_NAME.get());
    assertThat(testFolder).exists();

    File subFolder = new File(testFolder, CopyTo_Passes.FOLDER_NAME);
    assertThat(subFolder).exists();

    File file = new File(subFolder, CopyTo_Passes.FILE_NAME);
    assertThat(file).exists().hasContent(CopyTo_Passes.DATA);
  }

  @Test
  public void copyTo_whenPasses_doesNotCopyFolder_ifTestFails() {
    DESTINATION.set(temporaryFolder.getRoot());

    runTestWithExpectedFailureTypes(CopyTo_WhenPasses_Fails.class, ExpectedError.class);

    File destinationFolder = DESTINATION.get();
    assertThat(destinationFolder).exists();

    assertThat(destinationFolder.listFiles()).isEmpty();
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

  public abstract static class CopyTo_Passes {

    private static final String FOLDER_NAME = "myFolder";
    private static final String FILE_NAME = "myFile.txt";
    private static final String DATA = "data";

    @Rule
    public TestName testName = new TestName();

    @Before
    public void preCondition() {
      assertThat(DESTINATION.get()).exists();
    }

    protected abstract TemporaryFolder temporaryFolder();

    @Test
    public void writeToTemporaryFolder() throws Exception {
      TEST_NAME.set(testName.getMethodName());
      File folder = temporaryFolder().newFolder(FOLDER_NAME);
      File file = new File(folder, FILE_NAME);
      assertThat(file.createNewFile()).isTrue();
      FileUtils.writeStringToFile(file, DATA, Charset.defaultCharset());
    }
  }

  public abstract static class CopyTo_Fails {

    private static final String FOLDER_NAME = "myFolder";
    private static final String FILE_NAME = "myFile.txt";
    private static final String DATA = "data";

    @Rule
    public TestName testName = new TestName();

    @Before
    public void preCondition() {
      assertThat(DESTINATION.get()).exists();
    }

    protected abstract TemporaryFolder temporaryFolder();

    @Test
    public void writeToTemporaryFolder() throws Exception {
      TEST_NAME.set(testName.getMethodName());
      File folder = temporaryFolder().newFolder(FOLDER_NAME);
      File file = new File(folder, FILE_NAME);
      assertThat(file.createNewFile()).isTrue();
      FileUtils.writeStringToFile(file, DATA, Charset.defaultCharset());
      throw new ExpectedError(TEST_NAME.get() + " failed");
    }
  }

  public static class CopyTo_WhenAlways_Passes extends CopyTo_Passes {

    @Rule
    public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder()
        .copyTo(DESTINATION.get()).when(ALWAYS);

    @Override
    protected TemporaryFolder temporaryFolder() {
      return temporaryFolder;
    }
  }

  public static class CopyTo_WhenAlways_Fails extends CopyTo_Fails {

    @Rule
    public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder()
        .copyTo(DESTINATION.get()).when(ALWAYS);

    @Override
    protected TemporaryFolder temporaryFolder() {
      return temporaryFolder;
    }
  }

  public static class CopyTo_WhenFails_Passes extends CopyTo_Passes {

    @Rule
    public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder()
        .copyTo(DESTINATION.get()).when(FAILS);

    @Override
    protected TemporaryFolder temporaryFolder() {
      return temporaryFolder;
    }
  }

  public static class CopyTo_WhenFails_Fails extends CopyTo_Fails {

    @Rule
    public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder()
        .copyTo(DESTINATION.get()).when(FAILS);

    @Override
    protected TemporaryFolder temporaryFolder() {
      return temporaryFolder;
    }
  }

  public static class CopyTo_WhenPasses_Passes extends CopyTo_Passes {

    @Rule
    public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder()
        .copyTo(DESTINATION.get()).when(PASSES);

    @Override
    protected TemporaryFolder temporaryFolder() {
      return temporaryFolder;
    }
  }

  public static class CopyTo_WhenPasses_Fails extends CopyTo_Fails {

    @Rule
    public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder()
        .copyTo(DESTINATION.get()).when(PASSES);

    @Override
    protected TemporaryFolder temporaryFolder() {
      return temporaryFolder;
    }
  }

  @SuppressWarnings("serial")
  public static class ExpectedError extends AssertionError {

    ExpectedError(Object detailMessage) {
      super(detailMessage);
    }
  }
}
