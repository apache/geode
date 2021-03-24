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
package org.apache.geode.internal.cache.backup;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.DiskStore;
import org.apache.geode.deployment.internal.legacy.LegacyJarDeploymentService;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.DirectoryHolder;
import org.apache.geode.internal.cache.DiskInitFile;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.Oplog;
import org.apache.geode.internal.deployment.DeploymentServiceFactory;
import org.apache.geode.internal.deployment.JarDeploymentService;
import org.apache.geode.management.configuration.Deployment;
import org.apache.geode.test.compiler.JarBuilder;

public class BackupFileCopierIntegrationTest {
  private static final String CONFIG_DIRECTORY = "config";
  private static final String USER_FILES = "user";
  private static final String CACHE_XML = "cache.xml";
  private static final String PROPERTIES_FILE = "geode.properties";

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Rule
  public RestoreSystemProperties restoreProperties = new RestoreSystemProperties();

  private InternalCache cache;
  private TemporaryBackupFiles tempFiles;
  private Path tempFilesLocation;
  private BackupFileCopier fileCopier;
  private JarDeploymentService jarDeploymentService;

  @Before
  public void setup() throws IOException {
    jarDeploymentService = new LegacyJarDeploymentService();
    cache = mock(InternalCache.class);
    tempFiles = mock(TemporaryBackupFiles.class);
    tempFilesLocation = tempFolder.newFolder("temporaryBackupFiles").toPath();
    when(tempFiles.getDirectory()).thenReturn(tempFilesLocation);
    fileCopier = spy(new BackupFileCopier(cache, jarDeploymentService, tempFiles));
  }

  @After
  public void tearDown() {
    DeploymentServiceFactory.shutdownJarDeploymentService();
  }

  @Test
  public void configFilesNotCreatedIfDoNotExist() throws IOException {
    System.setProperty(DistributedSystem.PROPERTIES_FILE_PROPERTY,
        tempFolder.getRoot().getAbsolutePath() + SEPARATOR + "nonexistent");
    fileCopier.copyConfigFiles();
    Path cacheXmlDestination = tempFilesLocation.resolve(CONFIG_DIRECTORY).resolve(CACHE_XML);
    assertThat(cacheXmlDestination).doesNotExist();
    Path propertiesFileDestination =
        tempFilesLocation.resolve(CONFIG_DIRECTORY).resolve(PROPERTIES_FILE);
    assertThat(propertiesFileDestination).doesNotExist();
    assertThat(fileCopier.getBackupDefinition().getConfigFiles()).isEmpty();
  }

  @Test
  public void throwsIOExceptionIfConfigFileLocationInvalid()
      throws URISyntaxException, MalformedURLException {
    doReturn(new URL("http://www.test.com")).when(cache).getCacheXmlURL();
    doThrow(new URISyntaxException("test", "test")).when(fileCopier).getSource(any());
    assertThatThrownBy(() -> fileCopier.copyConfigFiles()).isInstanceOf(IOException.class);
  }

  @Test
  public void copiesConfigFilesToCorrectLocation() throws IOException {
    File propertiesFile = tempFolder.newFile(PROPERTIES_FILE);
    System.setProperty(DistributedSystem.PROPERTIES_FILE_PROPERTY,
        propertiesFile.getAbsolutePath());

    File cacheXml = tempFolder.newFile("cache.xml");
    URL cacheXmlURL = cacheXml.toURI().toURL();
    when(cache.getCacheXmlURL()).thenReturn(cacheXmlURL);

    fileCopier.copyConfigFiles();

    Path cacheXmlDestination = tempFilesLocation.resolve(CONFIG_DIRECTORY).resolve(CACHE_XML);
    assertThat(cacheXmlDestination).exists();
    Path propertiesFileDestination =
        tempFilesLocation.resolve(CONFIG_DIRECTORY).resolve(PROPERTIES_FILE);
    assertThat(propertiesFileDestination).exists();
    assertThat(fileCopier.getBackupDefinition().getConfigFiles())
        .containsExactlyInAnyOrder(cacheXmlDestination, propertiesFileDestination);
  }

  @Test
  public void noJarsInBackupIfNoneExist() throws IOException {
    fileCopier.copyDeployedJars();

    Path jarsDir = tempFilesLocation.resolve(USER_FILES);
    assertThat(Files.list(jarsDir)).isEmpty();
    assertThat(fileCopier.getBackupDefinition().getDeployedJars()).isEmpty();
  }

  @Test
  public void copiesDeployedJarsToCorrectLocation() throws IOException {
    File myJarFile = new File(tempFolder.getRoot().getCanonicalPath() + "/myJar.jar");
    JarBuilder jarBuilder = new JarBuilder();
    jarBuilder.buildJar(myJarFile, createClassContent("version1", "Abc"));
    Deployment deployment = new Deployment(myJarFile.getName(), "", Instant.now().toString());
    deployment.setFile(myJarFile);
    jarDeploymentService.deploy(deployment);
    fileCopier = new BackupFileCopier(cache, jarDeploymentService, tempFiles);

    Set<File> files = fileCopier.copyDeployedJars();
    files.forEach(file -> {
      try {
        System.err
            .println("BackupFileCopierIntegrationTest.copiesDeployedJarsToCorrectLocation:  "
                + file.getCanonicalPath());
      } catch (IOException e) {
        e.printStackTrace();
      }
    });

    Path expectedJar = tempFilesLocation.resolve(USER_FILES).resolve("myJar.v1.jar");
    assertThat(expectedJar).exists();
    assertThat(fileCopier.getBackupDefinition().getDeployedJars().keySet())
        .containsExactly(expectedJar);
    jarDeploymentService.undeployByDeploymentName("myJar");
  }

  @Test
  public void userDirectoryEmptyIfNoUserFiles() throws IOException {
    fileCopier.copyUserFiles();

    Path userDir = tempFilesLocation.resolve(USER_FILES);
    assertThat(Files.list(userDir)).isEmpty();
    assertThat(fileCopier.getBackupDefinition().getUserFiles()).isEmpty();
  }

  @Test
  public void userDirectoryContainsCorrectFiles() throws IOException {
    File userFile = tempFolder.newFile("userFile");
    File userSubdir = tempFolder.newFolder("userSubfolder");
    List<File> userFiles = new ArrayList<>();
    userFiles.add(userFile);
    userFiles.add(userSubdir);
    when(cache.getBackupFiles()).thenReturn(userFiles);

    fileCopier.copyUserFiles();

    Path expectedUserFile = tempFilesLocation.resolve(USER_FILES).resolve("userFile");
    Path expectedUserSubdir = tempFilesLocation.resolve(USER_FILES).resolve("userSubfolder");
    assertThat(expectedUserFile).exists();
    assertThat(expectedUserSubdir).exists();
    assertThat(fileCopier.getBackupDefinition().getUserFiles().keySet())
        .containsExactlyInAnyOrder(expectedUserFile, expectedUserSubdir);
  }

  @Test
  public void containsCorrectDiskInitFile() throws IOException {
    File initFileToCopy = tempFolder.newFile("initFile");
    DiskStoreImpl diskStore = mock(DiskStoreImpl.class);
    DiskInitFile initFile = mock(DiskInitFile.class);
    when(diskStore.getDiskInitFile()).thenReturn(initFile);
    when(diskStore.getInforFileDirIndex()).thenReturn(13);
    when(initFile.getIFFile()).thenReturn(initFileToCopy);

    fileCopier.copyDiskInitFile(diskStore);

    Path expectedInitFile = tempFilesLocation.resolve("13").resolve(initFileToCopy.getName());
    assertThat(expectedInitFile).exists();
    assertThat(fileCopier.getBackupDefinition().getDiskInitFiles())
        .containsExactly(entry(diskStore, expectedInitFile));
  }

  @Test
  public void copiesAllFilesForOplog() throws IOException {
    File oplogDir = tempFolder.newFolder("oplogDir");
    File crfFile = new File(oplogDir, "crf");
    File drfFile = new File(oplogDir, "drf");
    File krfFile = new File(oplogDir, "krf");
    Files.createFile(crfFile.toPath());
    Files.createFile(drfFile.toPath());
    Files.createFile(krfFile.toPath());

    Oplog oplog = mock(Oplog.class);
    DirectoryHolder dirHolder = mock(DirectoryHolder.class);
    when(dirHolder.getDir()).thenReturn(oplogDir);
    when(oplog.getDirectoryHolder()).thenReturn(dirHolder);
    when(oplog.getCrfFile()).thenReturn(crfFile);
    when(oplog.getDrfFile()).thenReturn(drfFile);
    when(oplog.getKrfFile()).thenReturn(krfFile);
    DiskStore diskStore = mock(DiskStore.class);
    when(tempFiles.getDiskStoreDirectory(any(), any()))
        .thenReturn(tempFolder.newFolder("diskstores").toPath());

    fileCopier.copyOplog(diskStore, oplog);

    Path expectedCrfFile =
        tempFiles.getDiskStoreDirectory(diskStore, dirHolder).resolve(crfFile.getName());
    Path expectedDrfFile =
        tempFiles.getDiskStoreDirectory(diskStore, dirHolder).resolve(drfFile.getName());
    Path expectedKrfFile =
        tempFiles.getDiskStoreDirectory(diskStore, dirHolder).resolve(krfFile.getName());
    assertThat(expectedCrfFile).exists();
    assertThat(expectedDrfFile).exists();
    assertThat(expectedKrfFile).exists();
    assertThat(fileCopier.getBackupDefinition().getOplogFilesByDiskStore())
        .containsOnlyKeys(diskStore);
    assertThat(fileCopier.getBackupDefinition().getOplogFilesByDiskStore().get(diskStore))
        .containsExactlyInAnyOrder(expectedCrfFile, expectedDrfFile, expectedKrfFile);
  }

  @Test
  public void copiesOplogFilesIfHardlinksFail() throws IOException {
    File oplogDir = tempFolder.newFolder("oplogDir");
    File crfFile = new File(oplogDir, "crf");
    File drfFile = new File(oplogDir, "drf");
    File krfFile = new File(oplogDir, "krf");
    Files.createFile(crfFile.toPath());
    Files.createFile(drfFile.toPath());
    Files.createFile(krfFile.toPath());

    Oplog oplog = mock(Oplog.class);
    DirectoryHolder dirHolder = mock(DirectoryHolder.class);
    when(dirHolder.getDir()).thenReturn(oplogDir);
    when(oplog.getDirectoryHolder()).thenReturn(dirHolder);
    when(oplog.getCrfFile()).thenReturn(crfFile);
    when(oplog.getDrfFile()).thenReturn(drfFile);
    when(oplog.getKrfFile()).thenReturn(krfFile);
    DiskStore diskStore = mock(DiskStore.class);
    Path diskStoreDir = tempFolder.newFolder("diskstores").toPath();
    when(tempFiles.getDiskStoreDirectory(any(), any())).thenReturn(diskStoreDir);

    doThrow(new IOException()).when(fileCopier).createLink(any(), any());

    fileCopier.copyOplog(diskStore, oplog);

    Path expectedCrfFile =
        tempFiles.getDiskStoreDirectory(diskStore, dirHolder).resolve(crfFile.getName());
    Path expectedDrfFile =
        tempFiles.getDiskStoreDirectory(diskStore, dirHolder).resolve(drfFile.getName());
    Path expectedKrfFile =
        tempFiles.getDiskStoreDirectory(diskStore, dirHolder).resolve(krfFile.getName());
    assertThat(expectedCrfFile).exists();
    assertThat(expectedDrfFile).exists();
    assertThat(expectedKrfFile).exists();
    assertThat(fileCopier.getBackupDefinition().getOplogFilesByDiskStore())
        .containsOnlyKeys(diskStore);
    assertThat(fileCopier.getBackupDefinition().getOplogFilesByDiskStore().get(diskStore))
        .containsExactlyInAnyOrder(expectedCrfFile, expectedDrfFile, expectedKrfFile);
  }

  private static String createClassContent(String version, String functionName) {
    return "package jddunit.function;" + "import org.apache.geode.cache.execute.Function;"
        + "import org.apache.geode.cache.execute.FunctionContext;" + "public class "
        + functionName + " implements Function {" + "public boolean hasResult() {return true;}"
        + "public String getId() {return \"" + version + "\";}"
        + "public void execute(FunctionContext context) {context.getResultSender().lastResult(\""
        + version + "\");}}";
  }
}
