/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.result;

import java.io.FileNotFoundException;
import java.io.IOException;

import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.cli.CliUtil;

/**
 * 
 * @author Abhishek Chaudhari
 * 
 * @since 7.0
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
