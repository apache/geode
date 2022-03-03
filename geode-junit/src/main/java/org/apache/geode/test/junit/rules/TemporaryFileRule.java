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
 *
 */

package org.apache.geode.test.junit.rules;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.junit.rules.ExternalResource;


/**
 * A {@link org.junit.rules.TestRule} to create temporary files in a given directory that should be
 * deleted when the test method finishes. This is useful in place of
 * {@link org.junit.rules.TemporaryFolder} when a test needs to create files in a particular
 * directory, for example user.home or user.dir.
 *
 * <p>
 * Example of usage:
 *
 * <pre>
 * public static class HasTemporaryFile {
 *   &#064;Rule
 *   public TemporaryFileRule temporaryFileRule = TemporaryFileRule.inUserHome();
 *
 *   &#064;Test
 *   public void testUsingTempFolder() throws IOException {
 *     File createdFile = temporaryFileRule.newFile(&quot;myfile.txt&quot;);
 *     File createdFile = temporaryFileRule.newFile(&quot;myfile2.txt&quot;);
 *     // ...
 *   }
 * }
 * </pre>
 */
public class TemporaryFileRule extends ExternalResource {

  private final String directory;

  private Set<File> files;

  private TemporaryFileRule(String parentDirectory) {
    directory = parentDirectory;
  }

  public static TemporaryFileRule inUserHome() {
    return new TemporaryFileRule(System.getProperty("user.home"));
  }

  public static TemporaryFileRule inCurrentDir() {
    return new TemporaryFileRule(System.getProperty("user.dir"));
  }

  public static TemporaryFileRule inDirectory(String directory) {
    return new TemporaryFileRule(directory);
  }

  @Override
  public void before() {
    files = new HashSet<>();
  }

  @Override
  public void after() {
    files.stream().filter(Objects::nonNull).filter(File::exists).forEach(File::delete);
  }

  /**
   * Creates a new file with the given name in the specified {@link #directory}.
   *
   * @param fileName the name of the file to create.
   *
   * @return the file that was created.
   *
   * @throws IllegalStateException if the file already exists.
   * @throws IllegalStateException if there is an {@link IOException} while creating the file.
   */
  public File newFile(String fileName) {
    return createFile(directory, fileName);
  }


  private File createFile(String directory, String fileName) {
    File file = new File(directory, fileName);
    try {
      if (!file.createNewFile()) {
        throw new IllegalStateException(
            "The specified file " + file.getAbsolutePath() + " already exists.");
      }
    } catch (IOException e) {
      throw new IllegalStateException(
          "IOException attempting to create file " + file.getAbsolutePath() + ".", e);
    }

    file.deleteOnExit();
    files.add(file);
    return file;
  }
}
