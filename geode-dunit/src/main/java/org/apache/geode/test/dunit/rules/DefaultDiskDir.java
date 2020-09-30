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
package org.apache.geode.test.dunit.rules;

import static org.apache.geode.TestContext.contextDirectory;
import static org.apache.geode.internal.lang.SystemPropertyHelper.DEFAULT_DISK_DIRS_PROPERTY;
import static org.apache.geode.internal.lang.SystemPropertyHelper.GEODE_PREFIX;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.io.FileUtils;

/**
 * Creates and cleans up a default disk directory for this JVM.
 */
public class DefaultDiskDir implements Serializable {
  private static final String PROPERTY_KEY = GEODE_PREFIX + DEFAULT_DISK_DIRS_PROPERTY;
  private File dir;

  /**
   * Creates a default disk directory and sets the associated system property to refer it. If the
   * default disk directory property already has a value, this method does nothing.
   *
   * @param suffix a suffix for the name of the directory
   * @throws IOException if an i/o error occurs
   */
  public void create(String suffix) throws IOException {
    if (System.getProperty(PROPERTY_KEY) != null) {
      // If the property is already set, some other code is managing the default disk dir.
      return;
    }
    dir = createDiskDirectory(suffix).toFile();
    System.setProperty(PROPERTY_KEY, dir.toString());
  }

  /**
   * Deletes any default disk directory and property created by this {@code DefaultDiskDir}.
   */
  public void delete() {
    if (dir != null) {
      System.clearProperty(PROPERTY_KEY);
      FileUtils.deleteQuietly(dir);
      dir = null;
    }
  }

  private Path createDiskDirectory(String suffix) throws IOException {
    String name = getClass().getSimpleName() + "-" + suffix;
    Path path = contextDirectory().resolve(name);
    return Files.createDirectories(path);
  }
}
