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
package org.apache.geode.internal.statistics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.net.URL;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.test.junit.categories.StatisticsTest;

/**
 * StatArchiveReader should throw IllegalStateException with detailed information instead of
 * throwing NullPointerException when encountering a Geode Stats file (.gfs) with a missing
 * ResourceType.
 *
 * <p>
 * {@code StatArchiveWithMissingResourceTypeRegressionTest.gfs} was hacked to have a missing
 * ResourceType. There is no way to generate an equivalent .gfs file.
 *
 * <p>
 * GEODE-2013: StatArchiveReader throws NullPointerException due to missing ResourceType
 */
@Category({StatisticsTest.class})
public class StatArchiveWithMissingResourceTypeRegressionTest {

  private static final String ARCHIVE_FILE_NAME =
      StatArchiveWithMissingResourceTypeRegressionTest.class.getSimpleName() + ".gfs";

  private File archiveFile;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    URL url = getClass().getResource(ARCHIVE_FILE_NAME);
    assertThat(url).isNotNull(); // precondition

    archiveFile = temporaryFolder.newFile(ARCHIVE_FILE_NAME);
    FileUtils.copyURLToFile(url, archiveFile);
    assertThat(archiveFile).exists(); // precondition
  }

  @After
  public void tearDown() throws Exception {
    StatisticsTypeFactoryImpl.clear();
  }

  @Test
  public void throwsIllegalStateExceptionWithMessage() throws Exception {
    assertThatThrownBy(() -> new StatArchiveReader(new File[] {archiveFile}, null, true))
        .isExactlyInstanceOf(IllegalStateException.class) // was NullPointerException
        .hasMessageStartingWith("ResourceType is missing for resourceTypeId 0")
        .hasMessageContaining("resourceName statistics1");
  }

}
