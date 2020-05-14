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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.functions.CliFunctionResult;

/**
 * This class is the primary container for results returned from a {@link GfshCommand}.
 * <br/>
 * The following different types of 'models' (or sections in the older terminology) can be added to
 * an instance of {@code ResultModel}.
 * <ul>
 * <li>{@code InfoResultModel}</li>
 * This model holds multiple lines of text.
 * <li>{@code TabularResultModel}</li>
 * This model holds a table of named columns and associated row values.
 * <li>{@code DataResultModel}</li>
 * This model holds a map of key/value pairs
 * </ul>
 * The order in which models are added is maintained and will be presented to the user in the same
 * order.
 * <br/>
 * Errors should just be added as {@code InfoResultModel}s and then the status should be set
 * appropriately to indicate an error. Once a {@code ResultModel}s status has been set to ERROR, it
 * cannot be reset to OK.
 * <br/>
 * Each added section should be given a descriptive name. This name will eventually become part of
 * the API when the JSON results are displayable via gfsh. So pick carefully :).
 * <br/>
 * A few common and generic names are already defined:
 * <ul>
 * <li>{@code INFO_SECTION}</li>
 * Used for sections created by the {@code addInfo()} and {@code createInfo} methods.
 * <li>{@code ERROR_SECTION}</li>
 * Used for sections created by the {@code createError()} method.
 * <li>{@code MEMBER_STATUS_SECTION}</li>
 * Used for sections created by the various {@code createMemberStatusResult()} methods.
 * </ul>
 *
 */
public class ResultModel {

  public static final String INFO_SECTION = "info";
  public static final String MEMBER_STATUS_SECTION = "member-status";

  private String header;
  private String footer;
  private Map<String, AbstractResultModel> sections = new LinkedHashMap<>();
  private Result.Status status = Result.Status.OK;
  private Object configObject;
  private List<FileResultModel> files = new ArrayList<>();

  @JsonIgnore
  public Object getConfigObject() {
    return configObject;
  }

  public void setConfigObject(Object configObject) {
    this.configObject = configObject;
  }

  @JsonIgnore
  public boolean isSuccessful() {
    return status == Result.Status.OK;
  }

  /**
   * Set the status of this {@code ResultModel}. Effectively, the only option is to set it to
   * ERROR. Once the state is set to ERROR it cannot be unset again.
   */
  public void setStatus(Result.Status status) {
    if (this.status == Result.Status.ERROR && status != this.status) {
      throw new IllegalStateException("Can't change the error state of the result.");
    }
    this.status = status;
  }

  public Result.Status getStatus() {
    return status;
  }

  public String getHeader() {
    return header;
  }

  public void setHeader(String header) {
    this.header = header;
  }

  public String getFooter() {
    return footer;
  }

  public void setFooter(String footer) {
    this.footer = footer;
  }

  public Map<String, AbstractResultModel> getContent() {
    return sections;
  }

  public void setContent(Map<String, AbstractResultModel> content) {
    this.sections = content;
  }

  public List<FileResultModel> getFiles() {
    return files;
  }

  public void setFiles(List<FileResultModel> files) {
    this.files = files;
  }

  /**
   * @param fileName only the name part of the file, no directory infomation
   * @param content the content to be saved to the file
   */
  public void addFile(String fileName, String content) {
    FileResultModel fileModel = new FileResultModel(fileName, content);
    files.add(fileModel);
  }

  public void addFile(File file, int fileType) {
    files.add(new FileResultModel(file, fileType));
  }

  /**
   * Overloaded method to create an {@code InfoResultModel} section called "info".
   */
  public InfoResultModel addInfo() {
    return addInfo(INFO_SECTION);
  }

  public InfoResultModel addInfo(String namedSection) {
    Object model = sections.get(namedSection);
    if (model != null) {
      if (model instanceof InfoResultModel) {
        return (InfoResultModel) model;
      } else {
        throw new IllegalStateException(String.format(
            "Section requested is %s, not InfoResultModel", model.getClass().getSimpleName()));
      }
    }

    InfoResultModel section = new InfoResultModel();
    sections.put(namedSection, section);

    return section;
  }

  @JsonIgnore
  public List<InfoResultModel> getInfoSections() {
    return sections.values().stream().filter(InfoResultModel.class::isInstance)
        .map(InfoResultModel.class::cast).collect(Collectors.toList());
  }

  public TabularResultModel addTable(String namedSection) {
    Object model = sections.get(namedSection);
    if (model != null) {
      if (model instanceof TabularResultModel) {
        return (TabularResultModel) model;
      } else {
        throw new IllegalStateException(String.format(
            "Section requested is %s, not TabularResultModel", model.getClass().getSimpleName()));
      }
    }

    TabularResultModel section = new TabularResultModel();
    sections.put(namedSection, section);

    return section;
  }

  public TabularResultModel addTableAndSetStatus(String namedSection,
      List<CliFunctionResult> functionResults, boolean ignoreIgnorable,
      boolean ignorePartialFailure) {
    Object model = sections.get(namedSection);
    if (model != null) {
      throw new IllegalStateException(
          "Section already exists. Can't overwrite it with this new content.");
    }
    TabularResultModel section = this.addTable(namedSection);
    boolean atLeastOneSuccess = false;
    boolean atLeastOneFailure = false;
    section.setColumnHeader("Member", "Status", "Message");
    for (CliFunctionResult functionResult : functionResults) {
      if (functionResult == null) {
        continue;
      }
      section.addRow(functionResult.getMemberIdOrName(), functionResult.getStatus(ignoreIgnorable),
          functionResult.getStatusMessage());
      if (functionResult.isSuccessful()) {
        atLeastOneSuccess = true;
      } else if (functionResult.isIgnorableFailure() && ignoreIgnorable) {
        atLeastOneSuccess = true;
      } else if (functionResult.isIgnorableFailure() && !ignoreIgnorable) {
        atLeastOneFailure = true;
      } else if (!functionResult.isSuccessful()) {
        atLeastOneFailure = true;
      }
    }
    if (ignorePartialFailure) {
      setStatus(atLeastOneSuccess ? Result.Status.OK : Result.Status.ERROR);
    } else {
      setStatus(atLeastOneFailure ? Result.Status.ERROR : Result.Status.OK);
    }
    return section;
  }

  @JsonIgnore
  public List<TabularResultModel> getTableSections() {
    return sections.values().stream().filter(TabularResultModel.class::isInstance)
        .map(TabularResultModel.class::cast).collect(Collectors.toList());
  }

  @JsonIgnore
  public int getSectionSize() {
    return sections.size();
  }

  public TabularResultModel getTableSection(String name) {
    return (TabularResultModel) getSection(name);
  }

  public InfoResultModel getInfoSection(String name) {
    return (InfoResultModel) getSection(name);
  }

  public AbstractResultModel getSection(String name) {
    return sections.get(name);
  }

  public DataResultModel addData(String namedSection) {
    Object model = sections.get(namedSection);
    if (model != null) {
      if (model instanceof DataResultModel) {
        return (DataResultModel) model;
      } else {
        throw new IllegalStateException(String.format(
            "Section requested is %s, not DataResultModel", model.getClass().getSimpleName()));
      }
    }

    DataResultModel section = new DataResultModel();
    sections.put(namedSection, section);

    return section;
  }

  @JsonIgnore
  public List<DataResultModel> getDataSections() {
    return sections.values().stream().filter(DataResultModel.class::isInstance)
        .map(DataResultModel.class::cast).collect(Collectors.toList());
  }

  public DataResultModel getDataSection(String name) {
    return (DataResultModel) getSection(name);
  }

  @JsonIgnore
  public List<String> getSectionNames() {
    List<String> sectionNames = new ArrayList<>();
    sections.forEach((k, v) -> sectionNames.add(k));
    return sectionNames;
  }


  public String toJson() {
    ObjectMapper mapper = new ObjectMapper();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      mapper.writeValue(baos, this);
    } catch (IOException e) {
      return e.getMessage();
    }
    return baos.toString();
  }

  @Override
  public String toString() {
    return toJson();
  }

  // ********************************************
  // static convenience methods
  // ********************************************

  public static ResultModel fromJson(String json) {
    ObjectMapper mapper = new ObjectMapper();

    ResultModel response;
    try {
      response = mapper.readValue(json, ResultModel.class);
    } catch (IOException iox) {
      throw new RuntimeException(iox);
    }
    return response;
  }

  /**
   * Helper method to create an {@code InfoResultModel} named "info". This method will also set
   * the status to ERROR.
   */
  public static ResultModel createError(String message) {
    ResultModel result = createInfo(message);
    result.setStatus(Result.Status.ERROR);
    return result;
  }

  /**
   * Helper method to create an {@code InfoResultModel} named "info".
   */
  public static ResultModel createInfo(String message) {
    ResultModel result = new ResultModel();
    result.addInfo(INFO_SECTION).addLine(message);
    result.setStatus(Result.Status.OK);
    return result;
  }

  public static ResultModel createMemberStatusResult(List<CliFunctionResult> functionResults,
      boolean ignoreIgnorable, boolean ignorePartialFailure) {
    return createMemberStatusResult(functionResults, null, null, ignoreIgnorable,
        ignorePartialFailure);
  }

  // this ignores the partial failure, but does not ignore the ignorable, if at least one success,
  // the command status is success
  public static ResultModel createMemberStatusResult(List<CliFunctionResult> functionResults) {
    return createMemberStatusResult(functionResults, null, null, false, true);
  }

  // this ignores the partial failure, if at least one function result is successful, the command
  // status is set to be successful
  public static ResultModel createMemberStatusResult(List<CliFunctionResult> functionResults,
      boolean ignoreIgnorable) {
    return createMemberStatusResult(functionResults, null, null, ignoreIgnorable, true);
  }


  /**
   * Helper method to create an {@code TabularResultModel} named "member-status". Typically used
   * to tabulate the status from calls to a number of members.
   */
  public static ResultModel createMemberStatusResult(List<CliFunctionResult> functionResults,
      String header, String footer, boolean ignoreIgnorable, boolean ignorePartialFailure) {
    ResultModel result = new ResultModel();

    TabularResultModel tabularResultModel =
        result.addTableAndSetStatus(MEMBER_STATUS_SECTION, functionResults, ignoreIgnorable,
            ignorePartialFailure);
    tabularResultModel.setHeader(header);
    tabularResultModel.setFooter(footer);
    return result;
  }

  /**
   * this saves the file data in this result model to the specified directory, and add appropriate
   * information to the result model to indicate the result of the file save.
   * this only applies to the commands that saves the file content in byte[], not download the
   * files over http channel
   *
   */
  public void saveFileTo(File dir) throws IOException {
    InfoResultModel info = addInfo("fileSave");
    if (files.size() == 0) {
      info.addLine("No file found to be saved.");
      setStatus(Result.Status.ERROR);
      return;
    }

    if (!dir.exists() && !dir.mkdirs()) {
      info.addLine(dir.getAbsolutePath() + " can not be created.");
      setStatus(Result.Status.ERROR);
      return;
    }
    if (!dir.isDirectory()) {
      info.addLine(dir.getAbsolutePath() + " is not a directory.");
      setStatus(Result.Status.ERROR);
      return;
    }
    if (!dir.canWrite()) {
      info.addLine("Can not write to " + dir.getAbsolutePath());
      setStatus(Result.Status.ERROR);
      return;
    }

    for (FileResultModel fileResult : files) {
      info.addLine(fileResult.saveFile(dir));
    }

  }

  @JsonIgnore
  public Path getFileToDownload() {
    if (files.size() != 1) {
      return null;
    }
    File file = files.get(0).getFile();
    if (file == null) {
      return null;
    }
    return file.toPath();
  }

  @JsonIgnore
  public List<File> getFileList() {
    return files.stream().filter(f -> f.getFile() != null).map(FileResultModel::getFile)
        .collect(
            Collectors.toList());
  }

  /**
   * Calculates the total file size of all files associated with this result.
   *
   * @return Total file size.
   */
  public long computeFileSizeTotal() {
    long byteCount = 0;
    for (FileResultModel file : files) {
      byteCount += file.getLength();
    }
    return byteCount;
  }

  /**
   * Get a comma separated list of all files associated with this result.
   *
   * @return Comma separated list of files.
   */
  @JsonIgnore
  public String getFormattedFileList() {
    return files.stream().map(FileResultModel::getFilename)
        .collect(Collectors.joining(", "));
  }
}
