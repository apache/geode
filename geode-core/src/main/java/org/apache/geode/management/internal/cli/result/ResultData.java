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

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.geode.management.cli.Result.Status;
import org.apache.geode.management.internal.cli.json.GfJsonObject;

/**
 * @since GemFire 7.0
 */
public interface ResultData {
  String RESULT_HEADER = "header";
  String RESULT_CONTENT = "content";
  String RESULT_FOOTER = "footer";

  String TYPE_COMPOSITE = "composite";
  String TYPE_ERROR = "error";
  String TYPE_INFO = "info";
  String TYPE_TABULAR = "table";
  String TYPE_MODEL = "model";

  String SECTION_DATA_ACCESSOR = "__sections__";
  String TABLE_DATA_ACCESSOR = "__tables__";
  String BYTE_DATA_ACCESSOR = "__bytes__";
  int FILE_TYPE_BINARY = 0;
  int FILE_TYPE_TEXT = 1;
  String FILE_NAME_FIELD = "fileName";
  String FILE_TYPE_FIELD = "fileType";
  String FILE_DATA_FIELD = "fileData";
  String DATA_LENGTH_FIELD = "dataLength";
  String FILE_MESSAGE = "fileMessage";

  String getHeader();

  String getFooter();

  @JsonIgnore
  default GfJsonObject getGfJsonObject() {
    throw new UnsupportedOperationException(
        "This should never be called and only exists during migration from GfJsonObject to POJOs - use getContent() instead");
  }

  @JsonIgnore
  default String getType() {
    throw new UnsupportedOperationException(
        "This should never be called and only exists during migration from GfJsonObject to POJOs");
  }

  @JsonIgnore
  default Status getStatus() {
    throw new UnsupportedOperationException(
        "This should never be called and only exists during migration from GfJsonObject to POJOs");
  }

  @JsonIgnore
  default void setStatus(final Status status) {
    throw new UnsupportedOperationException(
        "This should never be called and only exists during migration from GfJsonObject to POJOs");
  }

  default Object getContent() {
    throw new UnsupportedOperationException(
        "This should never be called from a legacy ResultData object");
  }
}
