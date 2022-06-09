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

import static java.lang.System.lineSeparator;
import static java.nio.file.Files.createDirectories;
import static java.nio.file.Files.isDirectory;
import static org.apache.commons.io.FileUtils.deleteDirectory;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.junit.rules.Folder.Policy.DELETE_ON_PASS;
import static org.apache.geode.test.junit.rules.Folder.Policy.KEEP_ALWAYS;

import java.io.IOException;
import java.nio.file.Path;

public class Folder {

  public enum Policy {
    DELETE_ON_PASS,
    KEEP_ALWAYS
  }

  private final Path root;
  private final Policy policy;

  public Folder(Policy policy, Path root) throws IOException {
    this.root = root.toAbsolutePath().normalize();
    this.policy = policy;

    if (this.policy == DELETE_ON_PASS && isDirectory(this.root)) {
      // folder already exists so caller does not own it and should not delete it
      throw new IllegalStateException(folderAlreadyExistsFailureMessage(this.root));
    }

    createDirectories(this.root);
  }

  private static String folderAlreadyExistsFailureMessage(Path root) {
    return String.join(lineSeparator(),
        "Folder already exists: " + root,
        "You must specify the order in which these rules are applied when using both:",
        "    @Rule(order = 0)",
        "    public FolderRule folderRule = new FolderRule();",
        "    @Rule(order = 1)",
        "    public GfshRule gfshRule = new GfshRule(folderRule::getFolder);");
  }

  public Path toPath() {
    return root;
  }

  public void testPassed() throws IOException {
    if (policy == KEEP_ALWAYS) {
      return;
    }
    await()
        .ignoreExceptions()
        .untilAsserted(() -> deleteDirectory(root.toFile()));
  }
}
