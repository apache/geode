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

package org.apache.geode.internal.deployment;

import static java.nio.file.Files.readAllBytes;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.management.internal.utils.JarFileUtils;
import org.apache.geode.test.compiler.ClassBuilder;

public class JarDeployerFileTest {
  private ClassBuilder classBuilder;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private File deploymentFolder;
  private File stagingFolder;
  private JarDeployer jarDeployer;

  @Before
  public void setup() throws IOException {
    classBuilder = new ClassBuilder();
    Path testFolder = temporaryFolder.getRoot().toPath().toAbsolutePath();
    deploymentFolder = Files.createDirectory(testFolder.resolve("deployment-folder")).toFile();
    stagingFolder = Files.createDirectory(testFolder.resolve("staging-folder")).toFile();
    jarDeployer = new JarDeployer(deploymentFolder);
  }

  @Test
  public void deployFileWithNoVersion_insertsDeploymentSequenceIdentifierIntoDeployedFileName()
      throws Exception {
    String artifactId = "jar-with-no-version";
    String extension = ".jar";
    String originalJarFileName = artifactId + extension;

    File version1JarFile = stagedJarFileWithClass(originalJarFileName, "ClassA");

    DeployedJar version1DeployedJar =
        jarDeployer.deployWithoutRegistering(artifactId, version1JarFile);

    String version1DeploymentSequenceIdentifier = ".v1";
    String expectedVersion1DeployedJarFileName =
        artifactId + version1DeploymentSequenceIdentifier + extension;

    assertThat(version1DeployedJar.getFile())
        .as("DeployedJar.getFile() for version 1 of %s", originalJarFileName)
        .exists()
        .hasName(expectedVersion1DeployedJarFileName)
        .hasBinaryContent(readAllBytes(version1JarFile.toPath()));

    File version2JarFile = stagedJarFileWithClass(originalJarFileName, "ClassB");

    DeployedJar version2DeployedJar =
        jarDeployer.deployWithoutRegistering(artifactId, version2JarFile);

    String version2DeploymentSequenceIdentifier = ".v2";
    String expectedVersion2DeployedJarFileName =
        artifactId + version2DeploymentSequenceIdentifier + extension;

    assertThat(version2DeployedJar.getFile())
        .as("DeployedJar.getFile() for version 2 of %s", originalJarFileName)
        .hasName(expectedVersion2DeployedJarFileName)
        .exists()
        .hasBinaryContent(readAllBytes(version2JarFile.toPath()));

    List<String> deployedFileNames = Files.list(deploymentFolder.toPath())
        .map(Path::getFileName)
        .map(Path::toString)
        .collect(toList());
    assertThat(deployedFileNames)
        .as("Deployed file names")
        .containsExactlyInAnyOrder(
            expectedVersion1DeployedJarFileName, expectedVersion2DeployedJarFileName);
  }

  @Test
  public void deployWithSemanticVersion_deploysFileUsingOriginalName() throws Exception {
    String artifactId = "jar-with-semantic-version";

    String version1JarName = artifactId + "-1.0.0.jar";
    File version1JarFile = stagedJarFileWithClass(version1JarName, "ClassA");

    DeployedJar version1DeployedJar =
        jarDeployer.deployWithoutRegistering(artifactId, version1JarFile);

    assertThat(version1DeployedJar.getFile())
        .as("DeployedJar.getFile() for jar file %s", version1JarName)
        .exists()
        .hasName("jar-with-semantic-version-1.0.0.v1.jar")
        .hasBinaryContent(readAllBytes(version1JarFile.toPath()));

    String version2JarName = artifactId + "-2.0.0.jar";
    File version2JarFile = stagedJarFileWithClass(version2JarName, "ClassA");

    DeployedJar version2DeployedJar =
        jarDeployer.deployWithoutRegistering(artifactId, version2JarFile);

    assertThat(version2DeployedJar.getFile())
        .as("DeployedJar.getFile() for jar file %s", version2JarName)
        .exists()
        .hasName("jar-with-semantic-version-2.0.0.v2.jar")
        .hasBinaryContent(readAllBytes(version2JarFile.toPath()));

    List<String> deployedFileNames = Files.list(deploymentFolder.toPath())
        .map(Path::getFileName)
        .map(Path::toString)
        .collect(toList());
    assertThat(deployedFileNames)
        .as("Deployed file names")
        .containsExactlyInAnyOrder("jar-with-semantic-version-1.0.0.v1.jar",
            "jar-with-semantic-version-2.0.0.v2.jar");
  }

  @Test
  public void toArtifactId() {
    // Semantic versions
    String fileName = "abc.1.v1.jar";
    assertThat(JarFileUtils.toArtifactId(fileName))
        .as("For filename %s", fileName)
        .isEqualTo("abc");
    fileName = "abc.1.1.v1.jar";
    assertThat(JarFileUtils.toArtifactId(fileName))
        .as("For filename %s", fileName)
        .isEqualTo("abc");
    fileName = "abc.1.1.1.v2.jar";
    assertThat(JarFileUtils.toArtifactId(fileName))
        .as("For filename %s", fileName)
        .isEqualTo("abc");
    fileName = "abc.1.1.1.1.1.1.1.1.1.1.1.v1.jar";
    assertThat(JarFileUtils.toArtifactId(fileName))
        .as("For filename %s", fileName)
        .isEqualTo("abc");
    fileName = "abc.1.SNAPSHOT.v2.jar";
    assertThat(JarFileUtils.toArtifactId(fileName))
        .as("For filename %s", fileName)
        .isEqualTo("abc");
    fileName = "abc.1.1.SNAPSHOT.v2.jar";
    assertThat(JarFileUtils.toArtifactId(fileName))
        .as("For filename %s", fileName)
        .isEqualTo("abc");
    fileName = "abc.1.1.1.SNAPSHOT.v2.jar";
    assertThat(JarFileUtils.toArtifactId(fileName))
        .as("For filename %s", fileName)
        .isEqualTo("abc");

    fileName = "abc1.1.1.1.v2.jar";
    assertThat(JarFileUtils.toArtifactId(fileName))
        .as("For filename %s", fileName)
        .isEqualTo("abc1");

    fileName = "abc-1.v2.jar";
    assertThat(JarFileUtils.toArtifactId(fileName))
        .as("For filename %s", fileName)
        .isEqualTo("abc");
    fileName = "abc-1.1.v2.jar";
    assertThat(JarFileUtils.toArtifactId(fileName))
        .as("For filename %s", fileName)
        .isEqualTo("abc");
    fileName = "abc-1.1.1.v2.jar";
    assertThat(JarFileUtils.toArtifactId(fileName))
        .as("For filename %s", fileName)
        .isEqualTo("abc");
    fileName = "abc-1.SNAPSHOT.v2.jar";
    assertThat(JarFileUtils.toArtifactId(fileName))
        .as("For filename %s", fileName)
        .isEqualTo("abc");
    fileName = "abc-1.1.SNAPSHOT.v2.jar";
    assertThat(JarFileUtils.toArtifactId(fileName))
        .as("For filename %s", fileName)
        .isEqualTo("abc");
    fileName = "abc-1.1.1.SNAPSHOT.v2.jar";
    assertThat(JarFileUtils.toArtifactId(fileName))
        .as("For filename %s", fileName)
        .isEqualTo("abc");

    fileName = "ab.c-1.1.1.v2.jar";
    assertThat(JarFileUtils.toArtifactId(fileName))
        .as("For filename %s", fileName)
        .isEqualTo("ab.c");

    fileName = "ab-c-1.1.1.v2.jar";
    assertThat(JarFileUtils.toArtifactId(fileName))
        .as("For filename %s", fileName)
        .isEqualTo("ab-c");

    fileName = "abc-1.0.RELEASE.2019.v2.jar";
    assertThat(JarFileUtils.toArtifactId(fileName))
        .as("For filename %s", fileName)
        .isEqualTo("abc");

    // Sequenced version scheme
    fileName = "abc.v92.jar";
    assertThat(JarFileUtils.toArtifactId(fileName))
        .as("For filename %s", fileName)
        .isEqualTo("abc");
    fileName = "ab.c.v92.jar";
    assertThat(JarFileUtils.toArtifactId(fileName))
        .as("For filename %s", fileName)
        .isEqualTo("ab.c");
    fileName = "abc.v1.v1.jar";
    assertThat(JarFileUtils.toArtifactId(fileName))
        .as("For filename %s", fileName)
        .isEqualTo("abc.v1");
    fileName = "ab-c.v92.jar";
    assertThat(JarFileUtils.toArtifactId(fileName))
        .as("For filename %s", fileName)
        .isEqualTo("ab-c");

    // Names where we do not recognize a version
    fileName = "abc-1.0.jar";
    assertThat(JarFileUtils.toArtifactId(fileName))
        .as("For filename %s", fileName)
        .isNull();
    fileName = "abc.jar";
    assertThat(JarFileUtils.toArtifactId(fileName))
        .as("For filename %s", fileName)
        .isNull();
    fileName = "abc.v92c.jar";
    assertThat(JarFileUtils.toArtifactId(fileName))
        .as("For filename %s", fileName)
        .isNull();
  }

  @Test
  public void getArtifactId() throws Exception {
    assertThat(JarFileUtils.getArtifactId("abc.jar")).isEqualTo("abc");
    assertThat(JarFileUtils.getArtifactId("abc-1.jar")).isEqualTo("abc");
    assertThat(JarFileUtils.getArtifactId("ab.c.1.jar")).isEqualTo("ab.c");
    assertThat(JarFileUtils.getArtifactId("abc.v1.jar")).isEqualTo("abc.v1");
    assertThat(JarFileUtils.getArtifactId("abc-1.0.snapshot.jar")).isEqualTo("abc");
    assertThat(JarFileUtils.getArtifactId("abc-1.0.v1.jar")).isEqualTo("abc");
    assertThat(JarFileUtils.getArtifactId("spark-network-common_2.11-2.3.1.jar"))
        .isEqualTo("spark-network-common_2");
  }

  @Test
  public void findArtifactIdsOnDisk_recognizesArtifactsByDeploymentSequenceIdentifier()
      throws IOException {
    String artifactId = "jar-with-sequence-identifier";
    String deploymentSequenceIdentifier = ".v3";
    String extension = ".jar";

    String fileNameWithSequenceIdentifier = artifactId + deploymentSequenceIdentifier + extension;

    givenDeployedFileNamed(fileNameWithSequenceIdentifier);

    assertThat(jarDeployer.findArtifactsAndMaxVersion()).hasSize(1)
        .containsEntry(artifactId, 3);
  }

  @Test
  public void findArtifactsOnDisk_recognizesArtifactsBySemanticVersion() throws IOException {
    String artifactId = "jar-with-semantic-version";
    String semanticVersion = "-2.3.4.v1";
    String extension = ".jar";

    String fileNameWithSequenceIdentifier = artifactId + semanticVersion + extension;

    givenDeployedFileNamed(fileNameWithSequenceIdentifier);

    assertThat(jarDeployer.findArtifactsAndMaxVersion()).hasSize(1)
        .containsEntry(artifactId, 1);
  }

  @Test
  public void findArtifactsOnDisk_excludesUnversionedFiles() throws IOException {
    givenDeployedFileNamed("jar-with-no-version.jar");

    assertThat(jarDeployer.findArtifactsAndMaxVersion()).isEmpty();
  }

  @Test
  public void testDeployToInvalidDirectory() throws Exception {
    final File alternateDir = new File(temporaryFolder.getRoot(), "JarDeployerDUnit");
    alternateDir.delete();

    final JarDeployer jarDeployer = new JarDeployer(alternateDir);
    final byte[] jarBytes = classBuilder.createJarFromName("JarDeployerDUnitDTID");
    File jarFile = stagedFileWithContent("JarDeployerIntegrationTest.jar", jarBytes);

    String artifactId = JarFileUtils.getArtifactId(jarFile.getName());

    // Test to verify that deployment fails if the directory doesn't exist.
    assertThatThrownBy(
        () -> jarDeployer.deployWithoutRegistering(JarFileUtils.getArtifactId(jarFile.getName()),
            jarFile))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unable to write to deploy directory:");
  }

  @Test
  public void testVersionNumberCreation() throws Exception {
    byte[] jarBytes = classBuilder.createJarFromName("ClassA");
    File jarFile = stagedFileWithContent("myJar.jar", jarBytes);
    File deployedJarFile = jarDeployer
        .deployWithoutRegistering(JarFileUtils.getArtifactId(jarFile.getName()), jarFile).getFile();

    assertThat(deployedJarFile.getName()).isEqualTo("myJar.v1.jar");

    File secondDeployedJarFile =
        jarDeployer.deployWithoutRegistering(JarFileUtils.getArtifactId(jarFile.getName()), jarFile)
            .getFile();

    assertThat(secondDeployedJarFile.getName()).isEqualTo("myJar.v2.jar");
  }

  @Test
  public void getNextVersionJarFileWithEmptyDir() throws Exception {
    File versionedName = jarDeployer.getNextVersionedJarFile("myJar.jar");
    assertThat(versionedName.getName()).isEqualTo("myJar.v1.jar");

    versionedName = jarDeployer.getNextVersionedJarFile("myJar.v1.jar");
    assertThat(versionedName.getName()).isEqualTo("myJar.v1.v1.jar");

    versionedName = jarDeployer.getNextVersionedJarFile("myJar-1.0.jar");
    assertThat(versionedName.getName()).isEqualTo("myJar-1.0.v1.jar");
  }

  @Test
  public void getNextVersionJarFileWithExistingFiles() throws Exception {
    Files.createFile(deploymentFolder.toPath().resolve("abc.jar"));
    Files.createFile(deploymentFolder.toPath().resolve("abc.v1.jar"));
    Files.createFile(deploymentFolder.toPath().resolve("myJar.v1.jar"));
    Files.createFile(deploymentFolder.toPath().resolve("myJar-1.0.v2.jar"));
    assertThat(jarDeployer.getMaxVersion("myJar")).isEqualTo(2);
    assertThat(jarDeployer.getMaxVersion("abc")).isEqualTo(1);
    assertThat(jarDeployer.getNextVersionedJarFile("myJar-2.0.jar").getName())
        .isEqualTo("myJar-2.0.v3.jar");

    Arrays.stream(deploymentFolder.listFiles()).forEach(FileUtils::deleteQuietly);
    Files.createFile(deploymentFolder.toPath().resolve("abc.jar"));
    Files.createFile(deploymentFolder.toPath().resolve("abc.v2.jar"));
    Files.createFile(deploymentFolder.toPath().resolve("myJar-1.0.v1.jar"));
    assertThat(jarDeployer.getNextVersionedJarFile("myJar.jar").getName())
        .isEqualTo("myJar.v2.jar");
  }

  @Test
  public void testVersionNumberMatcher() {
    String fileName = stagingFolder.toPath().resolve("MyJar.v1.jar").toAbsolutePath().toString();
    int version = JarFileUtils.extractVersionFromFilename(fileName);
    assertThat(version).isEqualTo(1);
  }

  @Test
  public void isSequenceVersion() throws Exception {
    assertThat(JarFileUtils.isDeployedFile("abc.v1.jar")).isTrue();
    assertThat(JarFileUtils.isDeployedFile("abc.v2.jar")).isTrue();
    assertThat(JarFileUtils.isDeployedFile("abc.jar")).isFalse();
    assertThat(JarFileUtils.isDeployedFile("abc-1.0.jar")).isFalse();
    assertThat(JarFileUtils.isDeployedFile("abc-1.0.v1.jar")).isTrue();
  }

  @Test
  public void isSemanticVersion() throws Exception {
    assertThat(JarFileUtils.isSemanticVersion("abc.v1.jar")).isFalse();
    assertThat(JarFileUtils.isSemanticVersion("abc.v2.jar")).isFalse();
    assertThat(JarFileUtils.isSemanticVersion("abc.jar")).isFalse();
    assertThat(JarFileUtils.isSemanticVersion("abc-1.0.jar")).isTrue();
    assertThat(JarFileUtils.isSemanticVersion("abc-1.0.v1.jar")).isTrue();
  }

  @Test
  public void testRenamingOfOldJarFiles() throws Exception {
    File jarAVersion1 = new File(deploymentFolder, "vf.gf#myJarA.jar#1");
    classBuilder.writeJarFromName("ClassA", jarAVersion1);

    File jarAVersion2 = new File(deploymentFolder, "vf.gf#myJarA.jar#2");
    classBuilder.writeJarFromName("ClassA", jarAVersion2);

    File jarBVersion2 = new File(deploymentFolder, "vf.gf#myJarB.jar#2");
    classBuilder.writeJarFromName("ClassB", jarBVersion2);

    File jarBVersion3 = new File(deploymentFolder, "vf.gf#myJarB.jar#3");
    classBuilder.writeJarFromName("ClassB", jarBVersion3);

    Set<File> deployedJarsBeforeRename = Stream
        .of(jarAVersion1, jarAVersion2, jarBVersion2, jarBVersion3).collect(toSet());

    jarDeployer.renameJarsWithOldNamingConvention();

    deployedJarsBeforeRename.forEach(oldJar -> assertThat(oldJar).doesNotExist());

    File renamedJarAVersion1 = new File(deploymentFolder, "myJarA.v1.jar");
    File renamedJarAVersion2 = new File(deploymentFolder, "myJarA.v2.jar");
    File renamedJarBVersion2 = new File(deploymentFolder, "myJarB.v2.jar");
    File renamedJarBVersion3 = new File(deploymentFolder, "myJarB.v3.jar");
    Set<File> expectedJarsAfterRename = Stream
        .of(renamedJarAVersion1, renamedJarAVersion2, renamedJarBVersion2, renamedJarBVersion3)
        .collect(toSet());

    Set<File> actualJarsInDeployDir = Files.list(deploymentFolder.toPath())
        .map(Path::toFile)
        .collect(toSet());

    assertThat(actualJarsInDeployDir).isEqualTo(expectedJarsAfterRename);
  }

  @Test
  public void testOldJarNameMatcher() {
    Set<File> filesWithOldNamingConvention = Stream.of(
        "vf.gf#myJarA.jar#1", "vf.gf#myJarA.jar#2", "vf.gf#myJarB.jar#2", "vf.gf#myJarB.jar#3")
        .map(this::deployedFileNamed)
        .collect(toSet());

    filesWithOldNamingConvention.forEach(
        jarFile -> assertThat(jarDeployer.isOldNamingConvention(jarFile.getName())).isTrue());

    Set<File> foundJarsWithOldNamingConvention = jarDeployer.findJarsWithOldNamingConvention();

    assertThat(foundJarsWithOldNamingConvention)
        .isEqualTo(filesWithOldNamingConvention);
  }

  @Test
  public void testDeleteAllVersionsOfJar() {
    File jarAVersion1 = deployedFileNamed("myJarA.v1.jar");
    File jarAVersion2 = deployedFileNamed("myJarA.v2.jar");
    File jarBVersion2 = deployedFileNamed("myJarB.v2.jar");
    File jarBVersion3 = deployedFileNamed("myJarB.v3.jar");

    jarDeployer.deleteAllVersionsOfJar("myJarA.jar");

    assertThat(jarAVersion1).doesNotExist();
    assertThat(jarAVersion2).doesNotExist();
    assertThat(jarBVersion2).exists();
    assertThat(jarBVersion3).exists();
  }

  @Test
  public void testDeleteAllVersionsOfVersionedJar() {
    File jarAVersion0 = deployedFileNamed("myJarABC-1.0.v1.jar");
    File jarAVersion1 = deployedFileNamed("myJarA-1.0.v1.jar");
    File jarAVersion2 = deployedFileNamed("myJarA.2.0.v2.jar");
    File jarAVersion3 = deployedFileNamed("myJarA.1.0-snapshot.v3.jar");
    File jarB = deployedFileNamed("myJarB.1.0.jar");

    jarDeployer.deleteAllVersionsOfJar("myJarA-2.0.jar");

    assertThat(jarAVersion0).exists();
    assertThat(jarAVersion1).doesNotExist();
    assertThat(jarAVersion2).doesNotExist();
    assertThat(jarAVersion3).doesNotExist();
    assertThat(jarB).exists();
  }

  private File deployedFileNamed(String fileName) {
    try {
      Path deployedFile = deploymentFolder.toPath().resolve(fileName);
      return Files.createFile(deployedFile).toFile();
    } catch (IOException e) {
      throw new RuntimeException("Test could not create deployed file " + fileName, e);
    }
  }

  private void givenDeployedFileNamed(String fileName) {
    deployedFileNamed(fileName);
  }

  private File stagedFileWithContent(String fileName, byte[] content) throws IOException {
    Path stagedFileDirectory = Files.createTempDirectory(stagingFolder.toPath(), "");
    Path stagedFilePath = stagedFileDirectory.resolve(fileName);
    Files.write(stagedFilePath, content);
    return stagedFilePath.toFile();
  }

  private File stagedJarFileWithClass(String fileName, String className) throws IOException {
    return stagedFileWithContent(fileName, jarContentWithClass(className));
  }

  private byte[] jarContentWithClass(String className) throws IOException {
    String stringBuilder = "package integration.parent;" + "public class " + className + " {}";

    return classBuilder.createJarFromClassContent("integration/parent/" + className,
        stringBuilder);
  }
}
