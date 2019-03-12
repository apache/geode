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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Time;
import java.text.MessageFormat;
import java.util.Base64;
import java.util.zip.DataFormatException;

import org.apache.geode.management.cli.Result.Status;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.CliUtil.DeflaterInflaterData;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.json.GfJsonArray;
import org.apache.geode.management.internal.cli.json.GfJsonException;
import org.apache.geode.management.internal.cli.json.GfJsonObject;
import org.apache.geode.management.internal.cli.shell.Gfsh;

/**
 *
 *
 * @since GemFire 7.0
 */
public abstract class AbstractResultData implements ResultData {

  protected GfJsonObject gfJsonObject;
  protected GfJsonObject contentObject;

  private Status status = Status.OK;

  protected AbstractResultData() {
    gfJsonObject = new GfJsonObject();
    contentObject = new GfJsonObject();
    try {
      gfJsonObject.putOpt(RESULT_CONTENT, contentObject);
    } catch (GfJsonException ignorable) {
      // ignorable as key won't be null here & it's thrown for ignorable values
    }
  }

  protected AbstractResultData(GfJsonObject jsonObject) {
    this.gfJsonObject = jsonObject;
    this.contentObject = gfJsonObject.getJSONObject(RESULT_CONTENT);
  }

  @Override
  public GfJsonObject getGfJsonObject() {
    return gfJsonObject;
  }

  @Override
  public abstract String getType();

  @Override
  public String getHeader() {
    return gfJsonObject.getString(RESULT_HEADER);
  }

  @Override
  public String getFooter() {
    return gfJsonObject.getString(RESULT_FOOTER);
  }

  /**
   *
   * @return this ResultData
   * @throws ResultDataException If the value is non-finite number or if the key is null.
   */
  public AbstractResultData setHeader(String headerText) {
    try {
      gfJsonObject.put(RESULT_HEADER, headerText);
    } catch (GfJsonException e) {
      throw new ResultDataException(e.getMessage());
    }

    return this;
  }

  /**
   *
   * @throws ResultDataException If the value is non-finite number or if the key is null.
   */
  public AbstractResultData setFooter(String footerText) {
    try {
      gfJsonObject.put(RESULT_FOOTER, footerText);
    } catch (GfJsonException e) {
      throw new ResultDataException(e.getMessage());
    }

    return this;
  }

  private static String addTimeStampBeforeLastDot(String src) {
    String toAdd = String.valueOf(new Time(System.currentTimeMillis()));
    toAdd = "-" + toAdd.replaceAll(":", "_");

    int lastIndexOf = src.lastIndexOf(".");
    if (lastIndexOf != -1) {
      String substr1 = src.substring(0, lastIndexOf);
      String substr2 = src.substring(lastIndexOf);

      src = substr1 + toAdd + substr2;
    } else {
      src = src + toAdd;
    }

    return src;
  }

  public ResultData addAsFile(String fileName, String fileContents, String message,
      boolean addTimeStampToName) {
    return this.addAsFile(fileName, fileContents.getBytes(), FILE_TYPE_TEXT, message,
        addTimeStampToName);
  }

  public ResultData addAsFile(String fileName, byte[] data, int fileType, String message,
      boolean addTimeStampToName) {
    if (addTimeStampToName) {
      fileName = addTimeStampBeforeLastDot(fileName);
    }
    return addAsFile(fileName, data, fileType, message);
  }

  private ResultData addAsFile(String fileName, byte[] data, int fileType, String message) {
    if (fileType != FILE_TYPE_BINARY && fileType != FILE_TYPE_TEXT) {
      throw new IllegalArgumentException("Unsupported file type is specified.");
    }

    GfJsonObject sectionData = new GfJsonObject();
    try {
      GfJsonArray fileDataArray = contentObject.getJSONArray(BYTE_DATA_ACCESSOR);
      if (fileDataArray == null) {
        fileDataArray = new GfJsonArray();
        contentObject.put(BYTE_DATA_ACCESSOR, fileDataArray);
      }
      fileDataArray.put(sectionData);

      sectionData.put(FILE_NAME_FIELD, fileName);
      sectionData.put(FILE_TYPE_FIELD, fileType);
      sectionData.put(FILE_MESSAGE, message);
      DeflaterInflaterData deflaterInflaterData = CliUtil.compressBytes(data);
      sectionData.put(FILE_DATA_FIELD,
          Base64.getEncoder().encodeToString(deflaterInflaterData.getData()));
      sectionData.put(DATA_LENGTH_FIELD, deflaterInflaterData.getDataLength());
    } catch (GfJsonException e) {
      throw new ResultDataException(e.getMessage());
    }
    return this;
  }

  public static void readFileDataAndDump(GfJsonArray byteDataArray, String directory)
      throws GfJsonException, DataFormatException, IOException {
    boolean overwriteAllExisting = false;
    int length = byteDataArray.size();
    String options = length > 1 ? "(y/N/a)" : "(y/N)";
    for (int i = 0; i < length; i++) {
      GfJsonObject object = byteDataArray.getInternalJsonObject(i);

      int fileType = object.getInt(FILE_TYPE_FIELD);

      if (fileType != FILE_TYPE_BINARY && fileType != FILE_TYPE_TEXT) {
        throw new IllegalArgumentException("Unsupported file type found.");
      }

      // build file name
      byte[] fileNameBytes;
      String fileName;
      GfJsonArray fileNameJsonBytes = object.getJSONArray(FILE_NAME_FIELD);
      if (fileNameJsonBytes != null) { // if in gfsh
        fileNameBytes = GfJsonArray.toByteArray(fileNameJsonBytes);
        fileName = new String(fileNameBytes);
      } else { // if on member
        fileName = object.getString(FILE_NAME_FIELD);
      }

      // build file message
      byte[] fileMessageBytes;
      String fileMessage;
      GfJsonArray fileMessageJsonBytes = object.getJSONArray(FILE_MESSAGE);
      if (fileMessageJsonBytes != null) { // if in gfsh
        fileMessageBytes = GfJsonArray.toByteArray(fileMessageJsonBytes);
        fileMessage = new String(fileMessageBytes);
      } else { // if on member
        fileMessage = object.getString(FILE_MESSAGE);
      }

      String fileDataString = object.getString(FILE_DATA_FIELD);
      int fileDataLength = object.getInt(DATA_LENGTH_FIELD);
      byte[] byteArray = Base64.getDecoder().decode(fileDataString);
      byte[] uncompressBytes = CliUtil.uncompressBytes(byteArray, fileDataLength).getData();

      File fileToDumpData = new File(fileName);
      if (!fileToDumpData.isAbsolute()) {
        if (directory == null || directory.isEmpty()) {
          directory = System.getProperty("user.dir", ".");
        }
        fileToDumpData = new File(directory, fileName);
      }

      File parentDirectory = fileToDumpData.getParentFile();
      if (parentDirectory != null) {
        parentDirectory.mkdirs();
      }
      Gfsh gfsh = Gfsh.getCurrentInstance();
      if (fileToDumpData.exists()) {
        String fileExistsMessage =
            CliStrings.format(CliStrings.ABSTRACTRESULTDATA__MSG__FILE_WITH_NAME_0_EXISTS_IN_1,
                fileName, fileToDumpData.getParent(), options);
        if (gfsh != null && !gfsh.isQuietMode() && !overwriteAllExisting) {
          fileExistsMessage = fileExistsMessage + " Overwrite? " + options + " : ";
          String interaction = gfsh.interact(fileExistsMessage);
          if ("a".equalsIgnoreCase(interaction.trim())) {
            overwriteAllExisting = true;
          } else if (!"y".equalsIgnoreCase(interaction.trim())) {
            // do not save file & continue
            continue;
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
      if (fileType == FILE_TYPE_TEXT) {
        FileWriter fw = new FileWriter(fileToDumpData);
        BufferedWriter bw = new BufferedWriter(fw);
        bw.write(new String(uncompressBytes));
        bw.flush();
        fw.flush();
        fw.close();
      } else if (fileType == FILE_TYPE_BINARY) {
        FileOutputStream fos = new FileOutputStream(fileToDumpData);
        fos.write(uncompressBytes);
        fos.flush();
        fos.close();
      }
      if (fileMessage != null && !fileMessage.isEmpty()) {
        if (gfsh != null) {
          Gfsh.println(MessageFormat.format(fileMessage, fileToDumpData.getAbsolutePath()));
        }
      }
    }
  }

  static void handleCondition(String message) throws IOException {
    Gfsh gfsh = Gfsh.getCurrentInstance();
    // null check required in GfshVM too to avoid test issues
    if (gfsh != null && !gfsh.isQuietMode()) {
      gfsh.logWarning(message, null);
    } else {
      throw new IOException(message);
    }
  }

  @Override
  public void setStatus(final Status status) {
    this.status = status;
  }

  @Override
  public Status getStatus() {
    return this.status;
  }
}
