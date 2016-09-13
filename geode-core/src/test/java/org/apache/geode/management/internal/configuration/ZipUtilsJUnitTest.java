/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.management.internal.configuration;

import static org.junit.Assert.*;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import com.gemstone.gemfire.management.internal.configuration.utils.ZipUtils;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * JUnit Test for {@link ZipUtils}
 */
@Category(IntegrationTest.class)
public class ZipUtilsJUnitTest {
  
  private String destinationFolderName = "destination";
  private String clusterFolderName = "cluster";
  private String groupFolderName = "group";
  private String clusterTextFileName = "cf.txt";
  private String groupTextFileName = "gf.txt";
  private String clusterText = "cluster content";
  private String groupText = "group content";

  private File sourceFolder;
  private File zipFolder;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    sourceFolder = temporaryFolder.newFolder("sourceFolder");

    File clusterFolder = new File(sourceFolder.getCanonicalPath(), clusterFolderName);
    assertTrue(clusterFolder.mkdir());

    File groupFolder = new File(sourceFolder.getCanonicalPath(), groupFolderName);
    assertTrue(groupFolder.mkdir());

    zipFolder = temporaryFolder.newFolder("zipFolder");

    FileUtils.writeStringToFile(new File(FilenameUtils.concat(clusterFolder.getCanonicalPath(), clusterTextFileName)), clusterText);
    FileUtils.writeStringToFile(new File(FilenameUtils.concat(groupFolder.getCanonicalPath(), groupTextFileName)), groupText);
  }

  @Test
  public void testZipUtils() throws Exception {
    File zipFile = new File(zipFolder, "target.zip");
    assertFalse(zipFile.exists());
    assertFalse(zipFile.isFile());

    ZipUtils.zip(sourceFolder.getCanonicalPath(), zipFile.getCanonicalPath());
    assertTrue(zipFile.exists());
    assertTrue(zipFile.isFile());

    File destinationFolder = new File(FilenameUtils.concat(temporaryFolder.getRoot().getCanonicalPath(), destinationFolderName));
    assertFalse(destinationFolder.exists());
    assertFalse(destinationFolder.isFile());

    ZipUtils.unzip(zipFile.getCanonicalPath(), destinationFolder.getCanonicalPath());
    assertTrue(destinationFolder.exists());
    assertTrue(destinationFolder.isDirectory());
    
    File[] destinationSubDirs = destinationFolder.listFiles();
    assertNotNull(destinationSubDirs);
    assertEquals(2, destinationSubDirs.length);

    File destinationClusterTextFile = new File(FilenameUtils.concat(destinationFolder.getCanonicalPath(), clusterFolderName + File.separator + clusterTextFileName));
    assertTrue(destinationClusterTextFile.exists());
    assertTrue(destinationClusterTextFile.isFile());

    File destinationGroupTextFile = new File(FilenameUtils.concat(destinationFolder.getCanonicalPath(), groupFolderName + File.separator + groupTextFileName));
    assertTrue(destinationGroupTextFile.exists());
    assertTrue(destinationGroupTextFile.isFile());

    assertTrue(clusterText.equals(FileUtils.readFileToString(destinationClusterTextFile)));
    assertTrue(groupText.equals(FileUtils.readFileToString(destinationGroupTextFile)));
  }
}
