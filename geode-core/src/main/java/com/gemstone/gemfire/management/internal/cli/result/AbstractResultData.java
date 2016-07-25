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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.zip.DataFormatException;

import com.gemstone.gemfire.management.cli.Result.Status;
import com.gemstone.gemfire.management.internal.cli.CliUtil;
import com.gemstone.gemfire.management.internal.cli.CliUtil.DeflaterInflaterData;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonArray;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonException;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonObject;
import com.gemstone.gemfire.management.internal.cli.shell.Gfsh;

/**
 * 
 * 
 * @since GemFire 7.0
 */
public abstract class AbstractResultData implements ResultData {
  public static final String SECTION_DATA_ACCESSOR = "__sections__";
  public static final String TABLE_DATA_ACCESSOR   = "__tables__";
  public static final String BYTE_DATA_ACCESSOR    = "__bytes__";

  public static final int FILE_TYPE_BINARY = 0;
  public static final int FILE_TYPE_TEXT   = 1;
  private static final String FILE_NAME_FIELD   = "fileName";
  private static final String FILE_TYPE_FIELD   = "fileType";
  private static final String FILE_DATA_FIELD   = "fileData";
  private static final String DATA_FIELD        = "data";
  private static final String DATA_LENGTH_FIELD = "dataLength";
  private static final String FILE_MESSAGE      = "fileMessage";
  
  protected GfJsonObject gfJsonObject;
  protected GfJsonObject contentObject;
  
  private Status status = Status.OK;
  
  protected AbstractResultData() {
    gfJsonObject  = new GfJsonObject();
    contentObject = new GfJsonObject();
    try {
      gfJsonObject.putOpt(RESULT_CONTENT, contentObject);
    } catch (GfJsonException ignorable) {
      //ignorable as key won't be null here & it's thrown for ignorable values
    }
  }
  
  protected AbstractResultData(GfJsonObject jsonObject) {
    this.gfJsonObject = jsonObject;
    this.contentObject = gfJsonObject.getJSONObject(RESULT_CONTENT);
  }

  /**
   * @return the gfJsonObject
   */
  public GfJsonObject getGfJsonObject() {
    return gfJsonObject;
  }

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
   * @param headerText
   * @return this ResultData
   * @throws ResultDataException
   *           If the value is non-finite number or if the key is null.
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
   * @param footerText
   * @return this ResultData
   * @throws ResultDataException
   *           If the value is non-finite number or if the key is null.
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
    String toAdd = String.valueOf(new java.sql.Time(System.currentTimeMillis()));
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

  public ResultData addAsFile(String fileName, String fileContents, String message, boolean addTimeStampToName) {
    return this.addAsFile(fileName, fileContents.getBytes(), FILE_TYPE_TEXT, message, addTimeStampToName);
  }
  
  public ResultData addAsFile(String fileName, byte[] data, int fileType, String message, boolean addTimeStampToName) {
    byte[] bytes = data;
    if (addTimeStampToName) {
      fileName = addTimeStampBeforeLastDot(fileName);
    }
    return addAsFile(fileName.getBytes(), bytes, fileType, message);
  }
  
  public ResultData addByteDataFromFileFile(String filePath, int fileType, String message, boolean addTimeStampToName) throws FileNotFoundException, IOException {
    byte[][] filesToBytes = CliUtil.filesToBytes(new String[] { filePath });
    
    byte[] bytes = filesToBytes[0];
    if (addTimeStampToName) {
      String fileName = new String(filesToBytes[0]);
      fileName = addTimeStampBeforeLastDot(fileName);
      bytes = fileName.getBytes();
    }
    return addAsFile(bytes, filesToBytes[1], fileType, message);
  }
  
  private ResultData addAsFile(byte[] fileName, byte[] data, int fileType, String message) {
//    System.out.println("fileType :: "+fileType);
//    System.out.println("FILE_TYPE_BINARY :: "+FILE_TYPE_BINARY);
//    System.out.println("FILE_TYPE_TEXT :: "+FILE_TYPE_TEXT);
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
      sectionData.put(FILE_MESSAGE, message.getBytes());
      sectionData.putAsJSONObject(FILE_DATA_FIELD, CliUtil.compressBytes(data));
//      System.out.println(data);
//      sectionData.put(FILE_DATA_FIELD, Base64.encodeBytes(data, Base64.GZIP));
    } catch (GfJsonException e) {
      throw new ResultDataException(e.getMessage());
//    } catch (IOException e) {
//      e.printStackTrace();
    }
    return this;
  }

  /**
   * @param byteDataArray
   * @throws GfJsonException
   * @throws DataFormatException
   * @throws IOException 
   */
  public static void readFileDataAndDump(GfJsonArray byteDataArray, String directory)
      throws GfJsonException, DataFormatException, IOException {
    boolean overwriteAllExisting = false;
    int length = byteDataArray.size();
    String options = length > 1 ? "(y/N/a)" : "(y/N)"; //TODO - Abhishek Make this consistent - with AbstractCliAroundInterceptor.readYesNo() 

    BYTEARRAY_LOOP:
    for (int i = 0; i < length; i++) {
      GfJsonObject object = byteDataArray.getJSONObject(i);

      int fileType = object.getInt(FILE_TYPE_FIELD);

      if (fileType != FILE_TYPE_BINARY && fileType != FILE_TYPE_TEXT) {
        throw new IllegalArgumentException("Unsupported file type found.");
      }

      // build file name
      byte[] fileNameBytes = null;
      GfJsonArray fileNameJsonBytes = object.getJSONArray(FILE_NAME_FIELD);
      if (fileNameJsonBytes != null) { // if in gfsh
        fileNameBytes = GfJsonArray.toByteArray(fileNameJsonBytes);
      } else { // if on member
        fileNameBytes = (byte[]) object.get(FILE_NAME_FIELD);
      }
      String fileName = new String(fileNameBytes);

      // build file message
      byte[] fileMessageBytes = null;
      GfJsonArray fileMessageJsonBytes = object.getJSONArray(FILE_MESSAGE);
      if (fileMessageJsonBytes != null) { // if in gfsh
        fileMessageBytes = GfJsonArray.toByteArray(fileMessageJsonBytes);
      } else { // if on member
        fileMessageBytes = (byte[]) object.get(FILE_MESSAGE);
      }
      String fileMessage = new String(fileMessageBytes);

  //      System.out.println(object.names());
//      System.out.println(fileName);

      GfJsonObject fileDataBytes = object.getJSONObject(FILE_DATA_FIELD);
      byte[] byteArray = GfJsonArray.toByteArray(fileDataBytes.getJSONArray(DATA_FIELD));
      int dataLength = fileDataBytes.getInt(DATA_LENGTH_FIELD);
      DeflaterInflaterData uncompressBytes = CliUtil.uncompressBytes(byteArray, dataLength);
      byte[] uncompressed = uncompressBytes.getData();

//      String encodedString = object.getString(FILE_DATA_FIELD);
//      byte[] uncompressed = Base64.decode(encodedString, Base64.GZIP);

      if (directory == null || directory.isEmpty()) {
        directory = System.getProperty("user.dir", ".");
      }

      boolean isGfshVM = CliUtil.isGfshVM();
      File fileToDumpData = new File(fileName);
      if (!fileToDumpData.isAbsolute()) {
        fileToDumpData = new File(directory, fileName);
      }
      File parentDirectory = fileToDumpData.getParentFile();
      if (fileToDumpData.exists()) {
        String fileExistsMessage = CliStrings.format(
                                    CliStrings.ABSTRACTRESULTDATA__MSG__FILE_WITH_NAME_0_EXISTS_IN_1,
                                    new Object[] { fileName, fileToDumpData.getParent(), options });
        if (isGfshVM) {
          Gfsh gfsh = Gfsh.getCurrentInstance();
          if (gfsh != null && !gfsh.isQuietMode() && !overwriteAllExisting) {
            fileExistsMessage = fileExistsMessage + " Overwrite? " + options + " : ";
            String interaction = gfsh.interact(fileExistsMessage);
            if ("a".equalsIgnoreCase(interaction.trim())) {
              overwriteAllExisting = true;
            } else if (!"y".equalsIgnoreCase(interaction.trim())) {
              // do not save file & continue
              continue BYTEARRAY_LOOP;
            }
          }
        } else {
          throw new IOException(fileExistsMessage);
        }
      } else if (!parentDirectory.exists()) {
        handleCondition(CliStrings.format(CliStrings.ABSTRACTRESULTDATA__MSG__PARENT_DIRECTORY_OF_0_DOES_NOT_EXIST, fileToDumpData.getAbsolutePath()), isGfshVM);
        return;
      } else if (!parentDirectory.canWrite()) {
        handleCondition(CliStrings.format(CliStrings.ABSTRACTRESULTDATA__MSG__PARENT_DIRECTORY_OF_0_IS_NOT_WRITABLE, fileToDumpData.getAbsolutePath()), isGfshVM);
        return;
      } else if (!parentDirectory.isDirectory()) {
        handleCondition(CliStrings.format(CliStrings.ABSTRACTRESULTDATA__MSG__PARENT_OF_0_IS_NOT_DIRECTORY, fileToDumpData.getAbsolutePath()), isGfshVM);
        return;
      }
      if (fileType == FILE_TYPE_TEXT) {
        FileWriter fw = new FileWriter(fileToDumpData);
        BufferedWriter bw = new BufferedWriter(fw);
        bw.write(new String(uncompressed));
        bw.flush();
        fw.flush();
        fw.close();
      } else if (fileType == FILE_TYPE_BINARY) {
        FileOutputStream fos = new FileOutputStream(fileToDumpData);
        fos.write(uncompressed);
        fos.flush();
        fos.close();
      }
//      System.out.println("fileMessage :: "+fileMessage);
      if (fileMessage != null && !fileMessage.isEmpty()) {
        if (isGfshVM) {
          Gfsh.println(MessageFormat.format(fileMessage, new Object[] {fileToDumpData.getAbsolutePath()}));
        }
      }
//      System.out.println(new String(uncompressed));
    }
  }

  //TODO - Abhishek : prepare common utility for this & ANSI Styling
  static void handleCondition(String message, boolean isGfshVM) throws IOException {
    if (isGfshVM) {
      Gfsh gfsh = Gfsh.getCurrentInstance();
      // null check required in GfshVM too to avoid test issues
      if (gfsh != null && !gfsh.isQuietMode()) {
        gfsh.logWarning(message, null);
      }
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
