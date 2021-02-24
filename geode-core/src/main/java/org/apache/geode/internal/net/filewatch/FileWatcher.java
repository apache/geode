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

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;

import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;

import org.apache.logging.log4j.Logger;

import org.apache.geode.logging.internal.log4j.api.LogService;

final class FileWatcher implements Runnable {
  private static final Logger logger = LogService.getLogger();

  private final Path path;
  private final Runnable callback;

  FileWatcher(Path path, Runnable callback) {
    this.path = path;
    this.callback = callback;
  }

  @Override
  public void run() {
    logger.info("Started watching {}", path);

    try {
      WatchService watchService = path.getFileSystem().newWatchService();

      // toAbsolutePath() ensures that the path will have a parent when calling getParent()
      Path parent = path.normalize().toAbsolutePath().getParent();
      parent.register(watchService, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);

      for (;;) {
        WatchKey key = watchService.take();

        for (WatchEvent<?> event : key.pollEvents()) {
          Path changed = (Path) event.context();

          if (!path.getFileName().equals(changed)) {
            // a different file has changed, ignoring
            continue;
          }

          callback.run();
        }

        boolean valid = key.reset();
        if (!valid) {
          logger.warn("Watch key is no longer valid for path {}", path);
          break;
        }
      }
    } catch (InterruptedException e) {
      logger.debug("Thread interrupted while watching {}", path);
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      logger.warn("Got exception while watching {}", path, e);
    }

    logger.info("Stopped watching {}", path);
  }
}
