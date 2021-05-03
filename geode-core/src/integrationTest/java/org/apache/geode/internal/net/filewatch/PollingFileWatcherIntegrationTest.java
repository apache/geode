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
package org.apache.geode.internal.net.filewatch;

import static java.nio.file.Files.createSymbolicLink;
import static java.nio.file.Files.getLastModifiedTime;
import static java.nio.file.Files.write;
import static java.time.Instant.now;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.nio.file.Path;
import java.time.Instant;

import org.apache.commons.lang3.SystemUtils;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class PollingFileWatcherIntegrationTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private Path watchedFile;

  @Before
  public void createWatchedFile() throws Exception {
    watchedFile = temporaryFolder.newFile("watched").toPath();
  }

  @Test
  public void followsSymbolicLinks() throws Exception {
    // Symbolic links require elevated permissions on Windows
    Assume.assumeFalse("Test ignored on Windows.", SystemUtils.IS_OS_WINDOWS);

    Path symlink = temporaryFolder.getRoot().toPath().resolve("symlink");
    createSymbolicLink(symlink, watchedFile);

    Runnable onUpdate = mock(Runnable.class);
    PollingFileWatcher watcher = new PollingFileWatcher(symlink, onUpdate, mock(Runnable.class));

    try {
      // Wait to ensure the timestamp after the update will be greater than before the update
      Instant lastModifiedTime = getLastModifiedTime(watchedFile).toInstant();
      await().until(() -> now().isAfter(lastModifiedTime.plusSeconds(5)));

      write(watchedFile, "update".getBytes());

      await().untilAsserted(() -> verify(onUpdate).run());

    } finally {
      watcher.stop();
    }
  }
}
