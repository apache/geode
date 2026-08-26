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

package org.apache.geode.management.internal.cli.functions;

import static org.apache.geode.management.internal.cli.functions.ExportDataFunction.EXPORT_DATA_DIRS_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests the export directories configured for a member, and which destinations resolve within
 * them.
 */
public class ExportDataDirectoryConfigTest {

  private static final String SNAPSHOT = "testRegion.gfd";

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private String originalProperty;
  private Path configuredDir;
  private Path otherDir;

  @Before
  public void before() throws Exception {
    originalProperty = System.getProperty(EXPORT_DATA_DIRS_PROPERTY);
    configuredDir = temporaryFolder.newFolder("exports").toPath().toRealPath();
    otherDir = temporaryFolder.newFolder("elsewhere").toPath().toRealPath();
    configure(configuredDir);
  }

  @After
  public void after() {
    if (originalProperty == null) {
      System.clearProperty(EXPORT_DATA_DIRS_PROPERTY);
    } else {
      System.setProperty(EXPORT_DATA_DIRS_PROPERTY, originalProperty);
    }
  }

  private void configure(Path... dirs) {
    StringBuilder value = new StringBuilder();
    for (Path dir : dirs) {
      if (value.length() > 0) {
        value.append(File.pathSeparator);
      }
      value.append(dir);
    }
    System.setProperty(EXPORT_DATA_DIRS_PROPERTY, value.toString());
  }

  @Test
  public void exportIntoConfiguredDirectorySucceeds() throws Exception {
    Path destination = configuredDir.resolve(SNAPSHOT);

    File resolved = ExportDataFunction.resolveExportFile(destination.toString());

    assertThat(resolved.toPath()).isEqualTo(destination);
  }

  @Test
  public void exportIntoSubdirectoryOfConfiguredDirectorySucceeds() throws Exception {
    Path destination = configuredDir.resolve("daily").resolve(SNAPSHOT);

    File resolved = ExportDataFunction.resolveExportFile(destination.toString());

    assertThat(resolved.toPath()).isEqualTo(destination);
  }

  @Test
  public void exportOutsideConfiguredDirectoriesIsRejected() {
    Path destination = otherDir.resolve(SNAPSHOT);

    assertThatThrownBy(() -> ExportDataFunction.resolveExportFile(destination.toString()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("not within the export directories configured")
        .hasMessageContaining(EXPORT_DATA_DIRS_PROPERTY);
  }

  @Test
  public void directoryWithMatchingNamePrefixIsNotIncluded() throws Exception {
    Path sibling = temporaryFolder.newFolder("exports-archive").toPath().toRealPath();
    Path destination = sibling.resolve(SNAPSHOT);

    assertThatThrownBy(() -> ExportDataFunction.resolveExportFile(destination.toString()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void severalDirectoriesCanBeConfigured() throws Exception {
    configure(configuredDir, otherDir);

    assertThat(ExportDataFunction.resolveExportFile(configuredDir.resolve(SNAPSHOT).toString()))
        .isNotNull();
    assertThat(ExportDataFunction.resolveExportFile(otherDir.resolve(SNAPSHOT).toString()))
        .isNotNull();
  }

  @Test
  public void emptyEntriesInThePropertyAreIgnored() throws Exception {
    System.setProperty(EXPORT_DATA_DIRS_PROPERTY,
        File.pathSeparator + configuredDir + File.pathSeparator + File.pathSeparator);

    File resolved =
        ExportDataFunction.resolveExportFile(configuredDir.resolve(SNAPSHOT).toString());

    assertThat(resolved.toPath()).isEqualTo(configuredDir.resolve(SNAPSHOT));
  }

  @Test
  public void workingDirectoryIsUsedWhenThePropertyIsNotSet() throws Exception {
    System.clearProperty(EXPORT_DATA_DIRS_PROPERTY);
    Path workingDir = new File(System.getProperty("user.dir")).getCanonicalFile().toPath();

    File resolved = ExportDataFunction.resolveExportFile(workingDir.resolve(SNAPSHOT).toString());

    assertThat(resolved.toPath()).isEqualTo(workingDir.resolve(SNAPSHOT));
    assertThatThrownBy(() -> ExportDataFunction.resolveExportFile(otherDir.resolve(SNAPSHOT)
        .toString())).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void relativePathResolvesInsideTheWorkingDirectory() throws Exception {
    System.clearProperty(EXPORT_DATA_DIRS_PROPERTY);
    Path workingDir = new File(System.getProperty("user.dir")).getCanonicalFile().toPath();

    File resolved = ExportDataFunction.resolveExportFile(SNAPSHOT);

    assertThat(resolved.toPath()).isEqualTo(workingDir.resolve(SNAPSHOT));
  }

  @Test
  public void linkedDirectoryResolvesToItsTarget() throws Exception {
    Path link = configuredDir.resolve("archive");
    try {
      Files.createSymbolicLink(link, otherDir);
    } catch (IOException | UnsupportedOperationException e) {
      Assume.assumeNoException("filesystem does not support links", e);
    }

    assertThatThrownBy(() -> ExportDataFunction.resolveExportFile(link.resolve(SNAPSHOT)
        .toString())).isInstanceOf(IllegalArgumentException.class);
  }
}
