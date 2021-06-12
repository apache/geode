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


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.WatchService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;

import org.apache.geode.InternalGemFireException;
import org.apache.geode.logging.internal.executors.LoggingExecutors;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Watches a single file for changes by polling the last modified time on the file.
 *
 * <p>
 * Note: {@link WatchService} is not used here because it has problems with container file systems
 * and remote file systems.
 * </p>
 */
final class PollingFileWatcher {
  private static final long PERIOD_SECONDS = 10;
  private static final Logger logger = LogService.getLogger();

  private final Path path;
  private final Runnable onUpdate;
  private final Runnable onError;
  private final ScheduledExecutorService executor;

  private long lastModifiedTimeMillis;

  PollingFileWatcher(Path path, Runnable onUpdate, Runnable onError) {
    this.path = path;
    this.onUpdate = onUpdate;
    this.onError = onError;

    try {
      lastModifiedTimeMillis = Files.getLastModifiedTime(path).toMillis();
    } catch (IOException e) {
      throw new InternalGemFireException("Unable to start watching " + path, e);
    }

    executor = LoggingExecutors.newSingleThreadScheduledExecutor(threadNameForPath(path));
    executor.scheduleAtFixedRate(this::poll, PERIOD_SECONDS, PERIOD_SECONDS, TimeUnit.SECONDS);

    logger.info("Started watching {}", path);
  }

  void stop() {
    executor.shutdown();
    logger.info("Stopped watching {}", path);
  }

  private void poll() {
    try {
      long timeStampMillis = Files.getLastModifiedTime(path).toMillis();
      if (timeStampMillis != lastModifiedTimeMillis) {
        logger.debug("Detected update for {}", path);
        lastModifiedTimeMillis = timeStampMillis;
        onUpdate.run();
      } else {
        logger.debug("No change detected for {}", path);
      }
    } catch (Exception e) {
      logger.debug("Error watching {}", path, e);
      onError.run();
    }
  }

  private static String threadNameForPath(Path path) {
    return String.format("file-watcher-%s-", path.getName(path.getNameCount() - 1));
  }
}
