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
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import org.apache.geode.management.cli.Result.Status;
import org.apache.geode.management.internal.cli.json.GfJsonObject;
import org.apache.geode.management.internal.cli.result.model.DataResultModel;
import org.apache.geode.management.internal.cli.result.model.InfoResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;

/**
 * @since GemFire 7.0
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
    property = "modelClass")
@JsonSubTypes({@JsonSubTypes.Type(value = TabularResultModel.class),
    @JsonSubTypes.Type(value = DataResultModel.class),
    @JsonSubTypes.Type(value = InfoResultModel.class)})
public interface ResultData {
  String RESULT_HEADER = "header";
  String RESULT_CONTENT = "content";
  String RESULT_FOOTER = "footer";


  String TYPE_COMPOSITE = "composite";
  String TYPE_ERROR = "error";
  String TYPE_INFO = "info";
  String TYPE_TABULAR = "table";

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

  default void setStatus(final Status status) {
    throw new UnsupportedOperationException(
        "This should never be called and only exists during migration from GfJsonObject to POJOs");
  }

  default Object getContent() {
    throw new UnsupportedOperationException(
        "This should never be called from a legacy ResultData object");
  }
}
