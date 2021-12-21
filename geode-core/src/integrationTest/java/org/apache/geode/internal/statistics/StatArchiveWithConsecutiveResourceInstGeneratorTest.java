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

import static org.apache.geode.internal.statistics.StatUtils.compareStatArchiveFiles;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.StatisticsTest;

/**
 * Compares the output results of {@link StatArchiveWithConsecutiveResourceInstGenerator} against
 * the saved stat archive file in src/test/resources that is used by
 * {@link StatArchiveWithConsecutiveResourceInstIntegrationTest}.
 *
 * @since Geode 1.0
 */
@Category({StatisticsTest.class})
public class StatArchiveWithConsecutiveResourceInstGeneratorTest
    extends StatArchiveWithConsecutiveResourceInstGenerator {

  private File expectedStatArchiveFile;

  @Before
  public void setUpGeneratorTest() throws Exception {
    URL url = getClass().getResource(ARCHIVE_FILE_NAME);
    File testFolder = temporaryFolder.newFolder(getClass().getSimpleName());
    expectedStatArchiveFile = new File(testFolder, ARCHIVE_FILE_NAME);
    FileUtils.copyURLToFile(url, expectedStatArchiveFile);
  }

  @Override
  protected void validateArchiveFile() throws IOException {
    compareStatArchiveFiles(expectedStatArchiveFile, new File(archiveFileName));
  }

}
