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
package org.apache.geode.internal.process.io;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Writes a string to the file.
 */
public class StringFileWriter {

  private final File file;

  public StringFileWriter(final File file) {
    this.file = file;
  }

  public void writeToFile(final String string) throws IOException {
    assertThat(file).doesNotExist();
    try (FileWriter writer = new FileWriter(file)) {
      writer.write(string);
      writer.flush();
    }
    assertThat(file).exists();
  }

}
