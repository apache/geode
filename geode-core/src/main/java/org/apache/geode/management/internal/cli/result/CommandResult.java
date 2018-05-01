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
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.json.GfJsonObject;

public interface CommandResult extends Result {

  Path getFileToDownload();

  boolean hasFileToDownload();

  Status getStatus();

  void setStatus(Status status);

  Object getResultData();

  Object getConfigObject();

  void setConfigObject(Object configObject);

  void resetToFirstLine();

  boolean hasIncomingFiles();

  int getNumTimesSaved();

  void saveIncomingFiles(String directory) throws IOException;

  boolean hasNextLine();

  String nextLine();

  String toJson();

  String getType();

  String getHeader();

  String getHeader(GfJsonObject gfJsonObject);

  GfJsonObject getContent();

  String getMessageFromContent();

  default String getErrorMessage() {
    throw new UnsupportedOperationException("This should never be called from LegacyCommandResult");
  }

  String getValueFromContent(String key);

  List<String> getListFromContent(String key);

  default List<String> getColumnFromTableContent(String column, String sectionId, String tableId) {
    throw new UnsupportedOperationException("This should never be called from ModelCommandResult");
  }

  default List<String> getColumnFromTableContent(String column, String tableId) {
    throw new UnsupportedOperationException("This should never be called from LegacyCommandResult");
  }

  default Map<String, List<String>> getMapFromTableContent(String sectionId, String tableId) {
    throw new UnsupportedOperationException("This should never be called from ModelCommandResult");
  }

  default Map<String, List<String>> getMapFromTableContent(String tableId) {
    throw new UnsupportedOperationException("This should never be called from LegacyCommandResult");
  }

  Map<String, String> getMapFromSection(String sectionID);

  String getFooter();

  boolean equals(Object obj);

  int hashCode();

  String toString();

  boolean failedToPersist();

  void setCommandPersisted(boolean commandPersisted);

  void setFileToDownload(Path fileToDownload);

  List<String> getTableColumnValues(String columnName);

  default List<String> getTableColumnValues(String sectionId, String columnName) {
    throw new UnsupportedOperationException("This should never be called from LegacyCommandResult");
  }
}
