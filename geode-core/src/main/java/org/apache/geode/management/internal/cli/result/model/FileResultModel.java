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

package org.apache.geode.management.internal.cli.result.model;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.text.MessageFormat;

import org.apache.commons.io.FileUtils;

import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ResultData;
import org.apache.geode.management.internal.cli.shell.Gfsh;

public class FileResultModel {
  public static int FILE_TYPE_BINARY = 0;
  public static int FILE_TYPE_TEXT = 1;

  private String filename;
  private int type;
  private String message;
  private byte[] data;
  private int length;

  public FileResultModel() {}

  public FileResultModel(String fileName, String content, String message) {
    this.filename = fileName;
    this.data = content.getBytes();
    this.length = data.length;
    this.type = FILE_TYPE_TEXT;
    this.message = message;
  }

  public FileResultModel(File file, int fileType) {
    if (fileType != FILE_TYPE_BINARY && fileType != FILE_TYPE_TEXT) {
      throw new IllegalArgumentException("Unsupported file type is specified.");
    }

    this.filename = file.getName();
    try {
      this.data = FileUtils.readFileToByteArray(file);
    } catch (IOException e) {
      throw new RuntimeException("Unable to read file: " + file.getAbsolutePath(), e);
    }
    this.length = data.length;
    this.type = fileType;
  }

  public String getFilename() {
    return filename;
  }

  public void setFilename(String filename) {
    this.filename = filename;
  }

  public int getType() {
    return type;
  }

  public void setType(int type) {
    this.type = type;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public byte[] getData() {
    return data;
  }

  public void setData(byte[] data) {
    this.data = data;
  }

  public int getLength() {
    return length;
  }

  public void setLength(int length) {
    this.length = length;
  }

  public void writeFile(String directory) throws IOException {
    String options = "(y/N)";

    File fileToDumpData = new File(filename);
    if (!fileToDumpData.isAbsolute()) {
      if (directory == null || directory.isEmpty()) {
        directory = System.getProperty("user.dir", ".");
      }
      fileToDumpData = new File(directory, filename);
    }

    File parentDirectory = fileToDumpData.getParentFile();
    if (parentDirectory != null) {
      parentDirectory.mkdirs();
    }
    Gfsh gfsh = Gfsh.getCurrentInstance();
    if (fileToDumpData.exists()) {
      String fileExistsMessage =
          CliStrings.format(CliStrings.ABSTRACTRESULTDATA__MSG__FILE_WITH_NAME_0_EXISTS_IN_1,
              filename, fileToDumpData.getParent(), options);
      if (gfsh != null && !gfsh.isQuietMode()) {
        fileExistsMessage = fileExistsMessage + " Overwrite? " + options + " : ";
        String interaction = gfsh.interact(fileExistsMessage);
        if (!"y".equalsIgnoreCase(interaction.trim())) {
          // do not save file & continue
          return;
        }
      } else {
        throw new IOException(fileExistsMessage);
      }
    } else if (!parentDirectory.exists()) {
      handleCondition(CliStrings.format(
          CliStrings.ABSTRACTRESULTDATA__MSG__PARENT_DIRECTORY_OF_0_DOES_NOT_EXIST,
          fileToDumpData.getAbsolutePath()));
      return;
    } else if (!parentDirectory.canWrite()) {
      handleCondition(CliStrings.format(
          CliStrings.ABSTRACTRESULTDATA__MSG__PARENT_DIRECTORY_OF_0_IS_NOT_WRITABLE,
          fileToDumpData.getAbsolutePath()));
      return;
    } else if (!parentDirectory.isDirectory()) {
      handleCondition(
          CliStrings.format(CliStrings.ABSTRACTRESULTDATA__MSG__PARENT_OF_0_IS_NOT_DIRECTORY,
              fileToDumpData.getAbsolutePath()));
      return;
    }
    if (type == ResultData.FILE_TYPE_TEXT) {
      FileWriter fw = new FileWriter(fileToDumpData);
      BufferedWriter bw = new BufferedWriter(fw);
      bw.write(new String(data));
      bw.flush();
      fw.flush();
      fw.close();
    } else if (type == ResultData.FILE_TYPE_BINARY) {
      FileOutputStream fos = new FileOutputStream(fileToDumpData);
      fos.write(data);
      fos.flush();
      fos.close();
    }
    if (message != null && !message.isEmpty()) {
      if (gfsh != null) {
        Gfsh.println(MessageFormat.format(message, fileToDumpData.getAbsolutePath()));
      }
    }
  }

  private void handleCondition(String message) throws IOException {
    Gfsh gfsh = Gfsh.getCurrentInstance();
    // null check required in GfshVM too to avoid test issues
    if (gfsh != null && !gfsh.isQuietMode()) {
      gfsh.logWarning(message, null);
    } else {
      throw new IOException(message);
    }
  }
}
