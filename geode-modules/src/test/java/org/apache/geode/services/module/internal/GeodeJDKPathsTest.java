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

package org.apache.geode.services.module.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

public class GeodeJDKPathsTest {
  @Rule
  public SystemPropertiesRestoreRule restoreProps = new SystemPropertiesRestoreRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  private File directory;
  private File jarDirectory;

  @Before
  public void setup() {
    directory = createDirectory(temporaryFolder.getRoot(), testName.getMethodName() + "1");
    jarDirectory = createDirectory(temporaryFolder.getRoot(), testName.getMethodName() + "2");
  }

  private File createDirectory(File parentDirectory, String name) {
    File file = new File(parentDirectory, name);
    assertThat(file.mkdir()).isTrue();
    return file;
  }

  @Test
  public void testsJDKPathWithSingleJar() throws IOException, URISyntaxException {
    URL testFileURL = ClassLoader.getSystemResource("Test.txt");
    File file1 = createFileForPath(directory + File.separator + "Test.txt", testFileURL);
    File file2 = createFileForPath(directory + File.separator + "level1" + File.separator + "level2"
        + File.separator + "level3" + File.separator + "Test.txt", testFileURL);

    File jarFile = createJarFileContainingFiles(file1, file2);

    System.setProperty("java.class.path", jarFile.getAbsolutePath());

    Set<String> jdkPaths = new TreeSet<>();
    GeodeJDKPaths.processClassPathItem(System.getProperty("java.class.path"), jdkPaths);
    assertThat(jdkPaths.size()).isEqualTo(1);
  }

  @Test
  public void testsJDKPathWithMultipleJarsWithUniquePaths() throws IOException, URISyntaxException {

    URL testFileURL = ClassLoader.getSystemResource("Test.txt");
    File file1 = createFileForPath(directory + File.separator + "Test.txt", testFileURL);
    File file2 = createFileForPath(directory + File.separator + "level1" + File.separator + "level2"
        + File.separator + "level3" + File.separator + "Test.txt", testFileURL);

    File file3 = createFileForPath(
        directory + File.separator + "level1" + File.separator + "Test.txt", testFileURL);
    File file4 = createFileForPath(directory + File.separator + "level1" + File.separator + "level2"
        + File.separator + "Test.txt", testFileURL);

    File jarFile1 = createJarFileContainingFiles(file1, file2);
    File jarFile2 = createJarFileContainingFiles(file3, file4);

    System.setProperty("java.class.path",
        jarFile1.getAbsolutePath() + File.pathSeparator + jarFile2.getAbsolutePath());

    Set<String> jdkPaths = new TreeSet<>();
    GeodeJDKPaths.processClassPathItem(System.getProperty("java.class.path"), jdkPaths);
    assertThat(jdkPaths.size()).isEqualTo(3);
    assertThat(jdkPaths).containsExactlyInAnyOrder("level1", "level1/level2",
        "level1/level2/level3");
  }

  @Test
  public void testsJDKPathWithMultipleJarsWithDuplicatePaths()
      throws IOException, URISyntaxException {

    URL testFileURL = ClassLoader.getSystemResource("Test.txt");
    File file1 = createFileForPath(directory + File.separator + "Test.txt", testFileURL);
    File file2 = createFileForPath(directory + File.separator + "level1" + File.separator + "level2"
        + File.separator + "level3" + File.separator + "Test.txt", testFileURL);

    File file3 = createFileForPath(
        directory + File.separator + "level1" + File.separator + "Test.txt", testFileURL);

    File file5 = createFileForPath(
        directory + File.separator + "level1" + File.separator + "Test.txt", testFileURL);
    File file6 = createFileForPath(directory + File.separator + "level1" + File.separator + "level2"
        + File.separator + "level3" + File.separator + "level4" + File.separator + "Test.txt",
        testFileURL);

    File jarFile1 = createJarFileContainingFiles(file1, file2);
    File jarFile2 = createJarFileContainingFiles(file3);
    File jarFile3 = createJarFileContainingFiles(file5, file6);

    System.setProperty("java.class.path",
        jarFile1.getAbsolutePath() + File.pathSeparator + jarFile2.getAbsolutePath()
            + File.pathSeparator + jarFile3.getAbsolutePath());

    Set<String> jdkPaths = new TreeSet<>();
    GeodeJDKPaths.processClassPathItem(System.getProperty("java.class.path"), jdkPaths);
    assertThat(jdkPaths.size()).isEqualTo(3);
    assertThat(jdkPaths).containsExactlyInAnyOrder("level1", "level1/level2/level3",
        "level1/level2/level3/level4");
  }

  @Test
  public void testsJDKPathWithSingleDirectory() throws IOException, URISyntaxException {
    URL testFileURL = ClassLoader.getSystemResource("Test.txt");
    File file1 = createFileForPath(directory + File.separator + "Test.txt", testFileURL);
    File file2 = createFileForPath(directory + File.separator + "level1" + File.separator + "level2"
        + File.separator + "level3" + File.separator + "Test.txt", testFileURL);

    System.setProperty("java.class.path", directory.getAbsolutePath() + File.separator);

    Set<String> jdkPaths = new TreeSet<>();
    GeodeJDKPaths.processClassPathItem(System.getProperty("java.class.path"), jdkPaths);
    assertThat(jdkPaths.size()).isEqualTo(1);
    assertThat(jdkPaths).containsExactlyInAnyOrder("level1/level2/level3");
  }

  @Test
  public void testsJDKPathWithMultipleDirectory() throws IOException, URISyntaxException {
    URL testFileURL = ClassLoader.getSystemResource("Test.txt");
    File file1 = createFileForPath(directory + File.separator + "Test.txt", testFileURL);
    File file2 = createFileForPath(directory + File.separator + "level1" + File.separator + "level2"
        + File.separator + "level3" + File.separator + "Test.txt", testFileURL);
    File file3 = createFileForPath(
        directory + File.separator + "level1" + File.separator + "Test.txt", testFileURL);
    File file4 = createFileForPath(directory + File.separator + "level1" + File.separator + "level2"
        + File.separator + "Test.txt", testFileURL);

    System.setProperty("java.class.path", directory.getAbsolutePath() + File.separator);

    Set<String> jdkPaths = new TreeSet<>();
    GeodeJDKPaths.processClassPathItem(System.getProperty("java.class.path"), jdkPaths);
    assertThat(jdkPaths.size()).isEqualTo(3);
    assertThat(jdkPaths).containsExactlyInAnyOrder("level1", "level1/level2",
        "level1/level2/level3");
  }

  @Test
  public void testsJDKPathWithJarsAndDirectories() throws IOException, URISyntaxException {
    URL testFileURL = ClassLoader.getSystemResource("Test.txt");
    File file1 = createFileForPath(directory + File.separator + "Test.txt", testFileURL);
    File file2 = createFileForPath(directory + File.separator + "level1" + File.separator + "level2"
        + File.separator + "level3" + File.separator + "Test.txt", testFileURL);

    File file3 = createFileForPath(
        jarDirectory + File.separator + "level1" + File.separator + "Test.txt", testFileURL);
    File file4 =
        createFileForPath(jarDirectory + File.separator + "level1" + File.separator + "level2"
            + File.separator + "Test.txt", testFileURL);

    File jarFile = createJarFileContainingFiles(file3, file4);

    System.setProperty("java.class.path",
        directory.getAbsolutePath() + File.pathSeparator + jarFile.getAbsolutePath());

    Set<String> jdkPaths = new TreeSet<>();
    GeodeJDKPaths.processClassPathItem(System.getProperty("java.class.path"), jdkPaths);
    assertThat(jdkPaths.size()).isEqualTo(3);
    assertThat(jdkPaths).containsExactlyInAnyOrder("level1", "level1/level2",
        "level1/level2/level3");
  }

  private File createFileForPath(String filePath, URL testFileURL)
      throws IOException, URISyntaxException {
    File file = new File(filePath);
    FileUtils.forceMkdirParent(file);
    FileUtils.copyFile(new File(testFileURL.toURI()), file);
    return file;
  }

  private File createJarFileContainingFiles(File... files) throws IOException {
    File jarFile = new File(
        jarDirectory.getPath() + File.separator + testName + new Random().nextLong() + ".jar");
    try (ZipOutputStream zipOutputStream = new ZipOutputStream(new FileOutputStream(jarFile))) {
      for (File fileToAdd : files) {
        zipOutputStream.putNextEntry(
            new ZipEntry(fileToAdd.getAbsolutePath().substring(directory.getPath().length() + 1)));
        FileUtils.copyFile(fileToAdd, zipOutputStream);
      }
    }
    return jarFile;
  }

}
