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

package org.apache.geode.test.junit.rules;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.commons.io.FileUtils.deleteQuietly;
import static org.apache.geode.test.TestRootDirectory.directoryOwnedBy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.ServerLauncher;
import org.apache.geode.test.junit.rules.serializable.SerializableExternalResource;

/**
 * A rule that configures and launches a server in this JVM. The rule automatically stops the server
 * and deletes its working directory.
 */
public class ServerLauncherBuilderRule extends SerializableExternalResource {
  private static final String DEFAULT_SERVER_NAME = "default-server";
  private static final String DEFAULT_LOG_LEVEL = "config";
  private static final int DEFAULT_SERVER_PORT = 0;
  private ServerLauncher.Builder builder;
  private ServerLauncher launcher;
  private Path workingDirectory;

  /**
   * Returns the builder that will build the server launcher.
   *
   * @return the server launcher builder
   */
  public ServerLauncher.Builder builder() {
    if (isNull(builder)) {
      builder = new ServerLauncher.Builder()
          .setServerPort(DEFAULT_SERVER_PORT)
          .setMemberName(DEFAULT_SERVER_NAME)
          .set(ConfigurationProperties.LOG_LEVEL, DEFAULT_LOG_LEVEL)
          .setWorkingDirectory(directoryOwnedBy(this).toString());
    }
    return builder;
  }

  /**
   * Returns the configured server launcher, building it if necessary, and creating its working
   * directory if necessary.
   * <p>
   * <b>NOTE:</b>
   * The launcher is built only on the first call to {@code launcher()}. Subsequent calls return the
   * same launcher instance.
   *
   * @return the configured server launcher
   */
  public ServerLauncher launcher() {
    if (isNull(launcher)) {
      workingDirectory = createWorkingDirectory(builder().getWorkingDirectory());
      launcher = builder().build();
    }
    return launcher;
  }

  @Override
  public void after() {
    if (nonNull(launcher)) {
      launcher.stop();
    }
    if (nonNull(workingDirectory)) {
      deleteQuietly(workingDirectory.toFile());
    }
  }

  private static Path createWorkingDirectory(String builderWorkingDirectory) {
    Path absolute = Paths.get(builderWorkingDirectory).toAbsolutePath().normalize();
    try {
      return Files.createDirectories(absolute);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
