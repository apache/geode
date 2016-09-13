/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.management.internal.cli.result;

import java.io.FileNotFoundException;
import java.io.IOException;

import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.cli.CliUtil;

/**
 * 
 * 
 * @since GemFire 7.0
 */
public class FileResult implements Result {
  private String[] filePaths;
  private int fileIndex;
  private Status status = Status.ERROR;
  private byte[][] localFileData = null;
  private boolean failedToPersist = false;

  public FileResult(String[] filePathsToRead) throws FileNotFoundException, IOException {
    this.filePaths = filePathsToRead;
    this.localFileData = CliUtil.filesToBytes(filePathsToRead);
    this.status = Status.OK;
  }

  @Override
  public Status getStatus() {
    return status;
  }

  @Override
  public void resetToFirstLine() {
  }

  @Override
  public boolean hasNextLine() {
    return fileIndex < filePaths.length;
  }

  @Override
  public String nextLine() {
    return filePaths[fileIndex++];
  }

  public byte[][] toBytes() {
    return localFileData;
  }

  /**
   * Calculates the total file size of all files associated with this result.
   * 
   * @return Total file size.
   */
  public long computeFileSizeTotal() {
    long byteCount = 0;
    for (int i = 1; i < this.localFileData.length; i += 2) {
      byteCount += localFileData[i].length;
    }
    return byteCount;
  }

  /**
   * Get a comma separated list of all files associated with this result.
   * 
   * @return Comma separated list of files.
   */
  public String getFormattedFileList() {
    StringBuffer formattedFileList = new StringBuffer();
    for (int i = 0; i < this.localFileData.length; i += 2) {
      formattedFileList.append(new String(this.localFileData[i]));
      if (i < this.localFileData.length - 2) {
        formattedFileList.append(", ");
      }
    }
    return formattedFileList.toString();
  }

  @Override
  public boolean hasIncomingFiles() {
    return true;
  }

  @Override
  public void saveIncomingFiles(String directory) 
      throws UnsupportedOperationException, IOException {
    // dump file data if any
    CliUtil.bytesToFiles(localFileData, directory, true);
  }

  @Override
  public boolean failedToPersist() {
    return this.failedToPersist;
  }

  @Override
  public void setCommandPersisted(boolean commandPersisted) {
    this.failedToPersist = !commandPersisted;
  }
}
