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

import static java.nio.file.LinkOption.NOFOLLOW_LINKS;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.WatchService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;

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
  private final Runnable callback;
  private final ScheduledExecutorService executor;

  private long lastModifiedTimeMillis;

  PollingFileWatcher(Path path, Runnable callback) {
    this.path = path;
    this.callback = callback;
    this.executor = LoggingExecutors.newSingleThreadScheduledExecutor(threadNameForPath(path));
  }

  void start() {
    try {
      lastModifiedTimeMillis = Files.getLastModifiedTime(path, NOFOLLOW_LINKS).toMillis();
    } catch (IOException e) {
      logger.warn("Unable to start watching {}", path, e);
      return;
    }

    executor.scheduleAtFixedRate(this::poll, PERIOD_SECONDS, PERIOD_SECONDS, TimeUnit.SECONDS);
    logger.info("Started watching {}", path);
  }

  void stop() {
    executor.shutdownNow();
    logger.info("Stopped watching {}", path);
  }

  private void poll() {
    long timeStampMillis;
    try {
      timeStampMillis = Files.getLastModifiedTime(path, NOFOLLOW_LINKS).toMillis();
    } catch (IOException e) {
      logger.warn("File does not exist or unable to get last modified time: {}", path, e);
      return;
    }

    if (timeStampMillis != lastModifiedTimeMillis) {
      logger.debug("Detected update for {}", path);
      lastModifiedTimeMillis = timeStampMillis;
      callback.run();
    } else {
      logger.debug("No change detected for {}", path);
    }
  }

  private static String threadNameForPath(Path path) {
    return String.format("FileWatch-%s-", path.getName(path.getNameCount() - 1));
  }
}
