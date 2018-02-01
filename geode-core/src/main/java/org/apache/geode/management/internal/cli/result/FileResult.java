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
package org.apache.geode.management.internal.cli.result;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.geode.management.cli.Result;

/**
 *
 *
 * @since GemFire 7.0
 */
public class FileResult implements Result {
  private List<File> files = new ArrayList<>();

  public void addFile(File file) {
    files.add(file);
  }

  @Override
  public Status getStatus() {
    return Status.OK;
  }

  @Override
  public void resetToFirstLine() {}

  @Override
  public boolean hasNextLine() {
    return false;
  }

  @Override
  public String nextLine() {
    return "";
  }

  public List<File> getFiles() {
    return files;
  }

  /**
   * Calculates the total file size of all files associated with this result.
   *
   * @return Total file size.
   */
  public long computeFileSizeTotal() {
    long byteCount = 0;
    for (File file : files) {
      byteCount += file.length();
    }
    return byteCount;
  }

  /**
   * Get a comma separated list of all files associated with this result.
   *
   * @return Comma separated list of files.
   */
  public String getFormattedFileList() {
    return files.stream().map(File::getName).collect(Collectors.joining(", "));
  }

  @Override
  public boolean hasIncomingFiles() {
    return true;
  }

  @Override
  public void saveIncomingFiles(String directory)
      throws UnsupportedOperationException, IOException {
    throw new UnsupportedOperationException("not supported");
  }

  @Override
  public boolean failedToPersist() {
    return false;
  }

  @Override
  public void setCommandPersisted(boolean commandPersisted) {
    throw new UnsupportedOperationException("not supported");
  }
}
