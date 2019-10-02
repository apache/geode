/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.management.internal.cli.shell;

import static java.lang.System.lineSeparator;
import static java.nio.charset.Charset.defaultCharset;
import static java.nio.file.Files.createDirectory;
import static java.nio.file.Files.createFile;
import static org.apache.commons.io.FileUtils.writeStringToFile;
import static org.apache.geode.management.internal.cli.shell.GfshConfig.DEFAULT_INIT_FILE_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class GfshConfigTest {

  private Path gfshHistoryFilePath;
  private Path gfshLogDirPath;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    gfshHistoryFilePath = temporaryFolder.getRoot().toPath().resolve("history").toAbsolutePath();
    gfshLogDirPath = temporaryFolder.getRoot().toPath().resolve("logs").toAbsolutePath();

    createFile(gfshHistoryFilePath);
    createDirectory(gfshLogDirPath);
  }

  @Test
  public void getInitFileNameReturnsNullIfNotProvided() {
    GfshConfig gfshConfig = new GfshConfig(gfshHistoryFilePath.toString(), "", 0,
        gfshLogDirPath.toString(), null, null, null, null);

    assertThat(gfshConfig.getInitFileName())
        .as("gfsh init file name")
        .isNull();
  }

  @Test
  public void getInitFileNameReturnsNameIfProvidedButDoesNotExist() {
    // Construct the file name but not the file
    Path initFilePath =
        temporaryFolder.getRoot().toPath().resolve(DEFAULT_INIT_FILE_NAME).toAbsolutePath();

    GfshConfig gfshConfig = new GfshConfig(gfshHistoryFilePath.toString(), "", 0,
        gfshLogDirPath.toString(), null, null, null, initFilePath.toString());

    assertThat(gfshConfig.getInitFileName())
        .as("gfsh init file name")
        .isNotNull();
  }

  @Test
  public void getInitFileNameReturnsNameIfProvidedButEmpty() throws Exception {
    Path initFilePath =
        temporaryFolder.getRoot().toPath().resolve(DEFAULT_INIT_FILE_NAME).toAbsolutePath();
    createFile(initFilePath);

    GfshConfig gfshConfig = new GfshConfig(gfshHistoryFilePath.toString(), "", 0,
        gfshLogDirPath.toString(), null, null, null, initFilePath.toString());

    assertThat(gfshConfig.getInitFileName())
        .as("gfsh init file name")
        .isNotNull();
  }

  @Test
  public void getInitFileNameReturnsNameIfProvidedWithGoodCommand() throws Exception {
    Path initFilePath =
        temporaryFolder.getRoot().toPath().resolve(DEFAULT_INIT_FILE_NAME).toAbsolutePath();
    writeStringToFile(initFilePath.toFile(), "echo --string=hello" + lineSeparator(),
        defaultCharset());

    GfshConfig gfshConfig = new GfshConfig(gfshHistoryFilePath.toString(), "", 0,
        gfshLogDirPath.toString(), null, null, null, initFilePath.toString());

    assertThat(gfshConfig.getInitFileName())
        .as("gfsh init file name")
        .isNotNull();
  }

  @Test
  public void getInitFileNameReturnsNameIfProvidedWithGoodCommands() throws Exception {
    Path initFilePath =
        temporaryFolder.getRoot().toPath().resolve(DEFAULT_INIT_FILE_NAME).toAbsolutePath();
    writeStringToFile(initFilePath.toFile(), "echo --string=hello" + lineSeparator(),
        defaultCharset());
    writeStringToFile(initFilePath.toFile(), "echo --string=goodbye" + lineSeparator(),
        defaultCharset(), true);

    GfshConfig gfshConfig = new GfshConfig(gfshHistoryFilePath.toString(), "", 0,
        gfshLogDirPath.toString(), null, null, null, initFilePath.toString());

    assertThat(gfshConfig.getInitFileName())
        .as("gfsh init file name")
        .isNotNull();
  }

  @Test
  public void getInitFileNameReturnsNameIfProvidedWithBadCommand() throws Exception {
    Path initFilePath =
        temporaryFolder.getRoot().toPath().resolve(DEFAULT_INIT_FILE_NAME).toAbsolutePath();
    writeStringToFile(initFilePath.toFile(), "fail" + lineSeparator(), defaultCharset());

    GfshConfig gfshConfig = new GfshConfig(gfshHistoryFilePath.toString(), "", 0,
        gfshLogDirPath.toString(), null, null, null, initFilePath.toString());

    assertThat(gfshConfig.getInitFileName())
        .as("gfsh init file name")
        .isNotNull();
  }

  @Test
  public void getInitFileNameReturnsNameIfProvidedWithBadCommands() throws Exception {
    Path initFilePath =
        temporaryFolder.getRoot().toPath().resolve(DEFAULT_INIT_FILE_NAME).toAbsolutePath();
    writeStringToFile(initFilePath.toFile(), "fail" + lineSeparator(), defaultCharset());
    writeStringToFile(initFilePath.toFile(), "fail" + lineSeparator(), defaultCharset(), true);

    GfshConfig gfshConfig = new GfshConfig(gfshHistoryFilePath.toString(), "", 0,
        gfshLogDirPath.toString(), null, null, null, initFilePath.toString());

    assertThat(gfshConfig.getInitFileName())
        .as("gfsh init file name")
        .isNotNull();
  }

  @Test
  public void getInitFileNameReturnsNameIfProvidedWithGoodAndBadCommands() throws Exception {
    Path initFilePath =
        temporaryFolder.getRoot().toPath().resolve(DEFAULT_INIT_FILE_NAME).toAbsolutePath();
    writeStringToFile(initFilePath.toFile(), "fail" + lineSeparator(), defaultCharset());
    writeStringToFile(initFilePath.toFile(), "echo --string=goodbye" + lineSeparator(),
        defaultCharset(), true);

    GfshConfig gfshConfig = new GfshConfig(gfshHistoryFilePath.toString(), "", 0,
        gfshLogDirPath.toString(), null, null, null, initFilePath.toString());

    assertThat(gfshConfig.getInitFileName())
        .as("gfsh init file name")
        .isNotNull();
  }
}
