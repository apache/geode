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

import org.apache.commons.io.FileUtils;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.net.URL;

@Category(IntegrationTest.class)
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

    this.archiveFile = this.temporaryFolder.newFile(ARCHIVE_FILE_NAME);
    FileUtils.copyURLToFile(url, this.archiveFile);
    assertThat(this.archiveFile).exists(); // precondition
  }

  @After
  public void tearDown() throws Exception {
    StatisticsTypeFactoryImpl.clear();
  }

  @Test // fixed GEODE-2013
  public void throwsIllegalStateExceptionWithMessage() throws Exception {
    assertThatThrownBy(() -> new StatArchiveReader(new File[] {this.archiveFile}, null, true))
        .isExactlyInstanceOf(IllegalStateException.class) // was NullPointerException
        .hasMessage("ResourceType is missing for resourceTypeId 0"); // was null
  }

}
