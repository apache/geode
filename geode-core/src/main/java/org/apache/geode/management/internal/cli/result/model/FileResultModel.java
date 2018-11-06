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

import org.apache.commons.io.FileUtils;

import org.apache.geode.management.internal.cli.result.ResultData;
import org.apache.geode.management.internal.cli.shell.Gfsh;

public class FileResultModel {
  public static int FILE_TYPE_BINARY = 0;
  public static int FILE_TYPE_TEXT = 1;

  private String filename;
  private int type;
  private byte[] data;
  private int length;

  public FileResultModel() {}

  public FileResultModel(String fileName, String content) {
    this.filename = fileName;
    this.data = content.getBytes();
    this.length = data.length;
    this.type = FILE_TYPE_TEXT;
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

  /**
   * at this point, the dir should already exist and is confirmed as a directory
   * filename in this instance should be file name only. no path in the file name
   *
   * @param directory the directory where to write the content of byte[] to with the filename
   * @return the message you would like to return to the user.
   */
  public String saveFile(File directory) throws IOException {
    String options = "(Y/N)";
    File file = new File(directory, filename).getAbsoluteFile();

    Gfsh gfsh = Gfsh.getCurrentInstance();
    if (file.exists()) {
      String fileExistsMessage = String.format("File with name \"%s\" already exists in \"%s\".",
          filename, directory.getAbsolutePath());
      if (gfsh != null && !gfsh.isQuietMode()) {
        fileExistsMessage += " Overwrite? " + options + " : ";
        String interaction = gfsh.interact(fileExistsMessage);
        if (!"y".equalsIgnoreCase(interaction.trim())) {
          // do not save file & continue
          return "User aborted. Did not overwrite " + file.getAbsolutePath();
        }
      } else {
        return fileExistsMessage;
      }
    }
    if (type == ResultData.FILE_TYPE_TEXT) {
      FileWriter fw = new FileWriter(file);
      BufferedWriter bw = new BufferedWriter(fw);
      bw.write(new String(data));
      bw.flush();
      fw.flush();
      fw.close();
    } else if (type == ResultData.FILE_TYPE_BINARY) {
      FileOutputStream fos = new FileOutputStream(file);
      fos.write(data);
      fos.flush();
      fos.close();
    }
    return "File saved to " + file.getAbsolutePath();
  }
}
