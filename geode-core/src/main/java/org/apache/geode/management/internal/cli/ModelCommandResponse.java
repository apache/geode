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
package org.apache.geode.management.internal.cli;


import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.geode.management.internal.cli.result.model.ResultModel;

public class ModelCommandResponse {

  private String sender;
  private String version;
  private int status;
  private String contentType;
  private String page;
  private String when;
  private String tokenAccessor;
  private String debugInfo;
  private ResultModel data;
  private boolean failedToPersist;
  private String fileToDownload;
  private boolean isLegacy;


  public ModelCommandResponse() {}

  public ResultModel getData() {
    return data;
  }

  public void setData(ResultModel data) {
    this.data = data;
  }

  public String getSender() {
    return sender;
  }

  public void setSender(String sender) {
    this.sender = sender;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public int getStatus() {
    return data.getStatus().getCode();
  }

  @JsonIgnore
  public void setStatus(int status) {
    this.status = status;
  }

  public String getContentType() {
    return contentType;
  }

  public void setContentType(String contentType) {
    this.contentType = contentType;
  }

  public String getPage() {
    return page;
  }

  public void setPage(String page) {
    this.page = page;
  }

  public String getWhen() {
    return when;
  }

  public void setWhen(String when) {
    this.when = when;
  }

  public String getTokenAccessor() {
    return tokenAccessor;
  }

  public void setTokenAccessor(String tokenAccessor) {
    this.tokenAccessor = tokenAccessor;
  }

  public String getDebugInfo() {
    return debugInfo;
  }

  public void setDebugInfo(String debugInfo) {
    this.debugInfo = debugInfo;
  }

  public boolean isFailedToPersist() {
    return failedToPersist;
  }

  public void setFailedToPersist(boolean failedToPersist) {
    this.failedToPersist = failedToPersist;
  }

  public String getFileToDownload() {
    return fileToDownload;
  }

  public void setFileToDownload(String fileToDownload) {
    this.fileToDownload = fileToDownload;
  }

  public boolean isLegacy() {
    return isLegacy;
  }

  public void setLegacy(boolean legacy) {
    isLegacy = legacy;
  }
}
