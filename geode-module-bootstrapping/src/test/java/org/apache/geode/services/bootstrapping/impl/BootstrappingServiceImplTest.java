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
package org.apache.geode.services.bootstrapping.impl;

import static org.apache.geode.services.result.impl.Success.SUCCESS_TRUE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.mockito.Mockito;

import org.apache.geode.services.bootstrapping.BootstrappingService;
import org.apache.geode.services.bootstrapping.internal.impl.BootstrappingServiceImpl;
import org.apache.geode.services.management.impl.ComponentIdentifier;
import org.apache.geode.services.module.ModuleService;
import org.apache.geode.services.result.ServiceResult;

public class BootstrappingServiceImplTest {
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  private File directory;
  private File jarDirectory;
  private ModuleService moduleServiceMock;
  private BootstrappingService bootstrappingService;

  @Before
  public void setup() {
    directory = createDirectory(temporaryFolder.getRoot(), testName.getMethodName() + "1");
    jarDirectory = createDirectory(temporaryFolder.getRoot(), testName.getMethodName() + "2");

    moduleServiceMock = Mockito.mock(ModuleService.class);
    when(moduleServiceMock.registerModule(any())).thenReturn(SUCCESS_TRUE);
    when(moduleServiceMock.loadModule(any())).thenReturn(SUCCESS_TRUE);
    when(moduleServiceMock.unregisterModule(any())).thenReturn(SUCCESS_TRUE);
    when(moduleServiceMock.unloadModule(any())).thenReturn(SUCCESS_TRUE);

    bootstrappingService = new BootstrappingServiceImpl();
    bootstrappingService.init(moduleServiceMock, LogManager.getLogger());
  }

  @Test
  public void testBootStrappingComponentWithoutPath() {
    ServiceResult<Boolean> result = bootstrappingService
        .bootStrapModule(new ComponentIdentifier("SomeModule"));
    assertThat(result.isFailure()).isTrue();
    assertThat(result.getErrorMessage()).contains("Invalid path for the component");
  }

  @Test
  public void testBootStrappingComponentWithEmptyPath() {
    ServiceResult<Boolean> result = bootstrappingService
        .bootStrapModule(new ComponentIdentifier("SomeModule", ""));
    assertThat(result.isFailure()).isTrue();
    assertThat(result.getErrorMessage()).contains("Invalid path for the component");

    result = bootstrappingService
        .bootStrapModule(new ComponentIdentifier("SomeModule", "         "));
    assertThat(result.isFailure()).isTrue();
    assertThat(result.getErrorMessage()).contains("Invalid path for the component");
  }

  @Test
  public void testBootStrappingComponentWithIncorrectPath() {
    ServiceResult<Boolean> result = bootstrappingService
        .bootStrapModule(new ComponentIdentifier("SomeModule", " ssdaffsf"));
    String[] errorMessageSnippet =
        new String[] {"java.io.FileNotFoundException:", "java.nio.file.NoSuchFileException:"};
    assertThat(result.isFailure()).isTrue();
    assertMessageContainsAny(result.getErrorMessage(), errorMessageSnippet);
  }

  @Test
  public void testBootStrappingComponentWithJarFilePathWithoutManifest()
      throws IOException, URISyntaxException {
    URL testFileURL = ClassLoader.getSystemResource("Test.txt");
    File file1 = createFileForPath(directory + File.separator + "Test.txt", testFileURL);
    File jarFile = createJarFileContainingFiles(file1);

    assertThat(bootstrappingService
        .bootStrapModule(new ComponentIdentifier("SomeModule", jarFile.getAbsolutePath()))
        .isSuccessful()).isTrue();
  }

  @Test
  public void testBootStrappingComponentWithJarFilePathWithManifestWithoutDependentModules()
      throws IOException, URISyntaxException {
    URL testFileURL = ClassLoader.getSystemResource("Test.txt");
    File file1 = createFileForPath(directory + File.separator + "Test.txt", testFileURL);
    File manifestFile = createManifestFile();
    File jarFile = createJarFileContainingFiles(file1, manifestFile);

    assertThat(bootstrappingService
        .bootStrapModule(new ComponentIdentifier("SomeModule", jarFile.getAbsolutePath()))
        .isSuccessful()).isTrue();
  }

  @Test
  public void testBootStrappingComponentWithJarFilePathWithManifestWithEmptyDependentModules()
      throws IOException, URISyntaxException {
    URL testFileURL = ClassLoader.getSystemResource("Test.txt");
    File file1 = createFileForPath(directory + File.separator + "Test.txt", testFileURL);
    File manifestFile = createManifestFile("Dependent-Modules:");
    File jarFile = createJarFileContainingFiles(file1, manifestFile);

    assertThat(bootstrappingService
        .bootStrapModule(new ComponentIdentifier("SomeModule", jarFile.getAbsolutePath()))
        .isSuccessful()).isTrue();
  }

  @Test
  public void testBootStrappingComponentWithJarFilePathWithManifestWithFictitiousDependentModules()
      throws IOException, URISyntaxException {
    URL testFileURL = ClassLoader.getSystemResource("Test.txt");
    File file1 = createFileForPath(directory + File.separator + "Test.txt", testFileURL);
    File manifestFile = createManifestFile("Dependent-Modules: fictitious-module");
    File jarFile = createJarFileContainingFiles(file1, manifestFile);

    ServiceResult<Boolean> result = bootstrappingService
        .bootStrapModule(new ComponentIdentifier("SomeModule", jarFile.getAbsolutePath()));
    String[] errorMessageSnippet =
        new String[] {"java.io.FileNotFoundException:", "java.nio.file.NoSuchFileException:"};
    assertThat(result.isFailure()).isTrue();
    assertMessageContainsAny(result.getErrorMessage(), errorMessageSnippet);
  }

  @Test
  public void testBootStrappingComponentWithJarFilePathWithManifestWithDependentModules()
      throws IOException, URISyntaxException {
    URL testFileURL = ClassLoader.getSystemResource("Test.txt");
    File file1 = createFileForPath(directory + File.separator + "Test.txt", testFileURL);

    File dependentModuleJarFile = createJarFileContainingFiles(file1);
    String dependentModuleJarFileName = dependentModuleJarFile.getName();
    File manifestFile = createManifestFile("Dependent-Modules: "
        + dependentModuleJarFileName.substring(0, dependentModuleJarFileName.indexOf(".jar")));

    File jarFile = createJarFileContainingFiles(file1, manifestFile);

    ServiceResult<Boolean> result = bootstrappingService
        .bootStrapModule(new ComponentIdentifier("SomeModule", jarFile.getAbsolutePath()));

    assertThat(result.isSuccessful()).isTrue();
  }

  private File createManifestFile(String... additionalManifestEntries) throws IOException {
    File manifestFile =
        new File(directory + File.separator + "META-INF" + File.separator + "manifest.mf");
    FileUtils.forceMkdirParent(manifestFile);
    manifestFile.createNewFile();
    try (FileWriter fileWriter = new FileWriter(manifestFile)) {
      fileWriter.write(getManifestEntryString());
      for (String additionalManifestEntry : additionalManifestEntries) {
        fileWriter.write(additionalManifestEntry + " \n");
      }
    }
    return manifestFile;
  }

  private File createDirectory(File parentDirectory, String name) {
    File file = new File(parentDirectory, name);
    assertThat(file.mkdir()).isTrue();
    return file;
  }

  private File createJarFileContainingFiles(File... files) throws IOException {
    File jarFile = new File(
        jarDirectory.getPath() + File.separator + testName + new Random().nextLong() + ".jar");
    try (JarOutputStream zipOutputStream = new JarOutputStream(new FileOutputStream(jarFile))) {
      for (File fileToAdd : files) {
        zipOutputStream.putNextEntry(
            new ZipEntry(fileToAdd.getAbsolutePath().substring(directory.getPath().length() + 1)));
        FileUtils.copyFile(fileToAdd, zipOutputStream);
      }
    }
    return jarFile;
  }

  private File createFileForPath(String filePath, URL testFileURL)
      throws IOException, URISyntaxException {
    File file = new File(filePath);
    FileUtils.forceMkdirParent(file);
    FileUtils.copyFile(new File(testFileURL.toURI()), file);
    return file;
  }

  private String getManifestEntryString() {
    return "Manifest-Version: 1.0\n"
        + "Organization: Apache Software Foundation (ASF)\n"
        + "Module-Name: testModule\n"
        + "Class-Path: some.jar\n"
        + "Title: geode\n"
        + "Version: xx.xxx.xxx\n"
        + "Created-By: coder\n";
  }

  private void assertMessageContainsAny(String errorMessage, String[] errorMessageSnippet) {
    AtomicBoolean containsString = new AtomicBoolean();
    Arrays.stream(errorMessageSnippet)
        .forEach(errorSnippet -> {
          boolean contains = errorMessage.contains(errorSnippet);
          if (contains) {
            containsString.set(true);
          }
        });
    assertThat(containsString.get()).isTrue();
  }
}
