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

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import org.junit.rules.ExternalResource;

/**
 * A rule that creates a temporary directory and cleans it up after the test.
 */
public class DiskDirRule extends ExternalResource {
  private File diskDir;

  @Override
  protected void before() throws Throwable {
    diskDir = new File(".", "DiskDirRule-" + System.nanoTime());
  }

  @Override
  protected void after() {
    if (!diskDir.exists()) {
      return;
    }

    try {
      Files.walk(diskDir.toPath()).forEach((path) -> {
        try {
          Files.delete(path);
        } catch (IOException e) {
          // Ignore
        }
      });
    } catch (IOException e) {
      throw new RuntimeException("Could not delete disk dir: " + diskDir, e);
    }
    diskDir.delete();
  }

  public File get() {
    diskDir.mkdirs();
    return diskDir;
  }
}
