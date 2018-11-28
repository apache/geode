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

import java.io.IOException;

import org.apache.commons.lang3.Validate;

import org.apache.geode.management.cli.Result;

public class DownloadFileResult implements Result {
  private String filePath = null;
  private boolean hasLine = true;

  public DownloadFileResult(String filePath) {
    Validate.notNull(filePath);
    this.filePath = filePath;
  }

  @Override
  public Status getStatus() {
    return Status.OK;
  }

  @Override
  public void resetToFirstLine() {}

  @Override
  public boolean hasNextLine() {
    return hasLine;
  }

  @Override
  public String nextLine() {
    if (hasLine) {
      hasLine = false;
      return filePath;
    }

    throw new IndexOutOfBoundsException();
  }

  @Override
  public boolean hasIncomingFiles() {
    return true;
  }

  @Override
  public void saveIncomingFiles(String directory) throws IOException {}

  @Override
  public boolean failedToPersist() {
    return false;
  }

  @Override
  public void setCommandPersisted(boolean commandPersisted) {

  }
}
