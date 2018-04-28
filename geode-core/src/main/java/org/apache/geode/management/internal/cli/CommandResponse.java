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

import java.nio.file.Path;
import java.text.DateFormat;

import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.management.internal.cli.json.GfJsonObject;
import org.apache.geode.management.internal.cli.result.ResultData;

/**
 * @since GemFire 7.0
 */
public class CommandResponse {

  private final String sender;
  private final String version;
  private final int status;
  private final String contentType;
  private final String page;
  private final String when;
  private final String tokenAccessor;
  private final String debugInfo;
  private final ResultData data;
  private final boolean failedToPersist;
  private final String fileToDownload;
  private final boolean isLegacy;

  CommandResponse(String sender, String contentType, int status, String page, String tokenAccessor,
      String debugInfo, String header, GfJsonObject content, String footer, boolean failedToPersist,
      Path fileToDownload) {
    this.sender = sender;
    this.contentType = contentType;
    this.status = status;
    this.page = page;
    this.tokenAccessor = tokenAccessor;
    this.debugInfo = debugInfo;
    this.data = new LegacyData(header, content, footer);
    this.when = DateFormat.getInstance().format(new java.util.Date());
    this.version = GemFireVersion.getGemFireVersion();
    this.failedToPersist = failedToPersist;
    if (fileToDownload != null) {
      this.fileToDownload = fileToDownload.toString();
    } else {
      this.fileToDownload = null;
    }
    this.isLegacy = true;
  }

  // For de-serializing
  CommandResponse(GfJsonObject jsonObject) {
    this.sender = jsonObject.getString("sender");
    this.contentType = jsonObject.getString("contentType");
    this.status = jsonObject.getInt("status");
    this.page = jsonObject.getString("page");
    this.tokenAccessor = jsonObject.getString("tokenAccessor");
    this.debugInfo = jsonObject.getString("debugInfo");
    this.data = new LegacyData(jsonObject.getJSONObject("data"));
    this.when = jsonObject.getString("when");
    this.version = jsonObject.getString("version");
    this.failedToPersist = jsonObject.getBoolean("failedToPersist");
    this.fileToDownload = jsonObject.getString("fileToDownload");
    this.isLegacy = true;
  }

  /**
   * @return the sender
   */
  public String getSender() {
    return sender;
  }

  /**
   * @return the version
   */
  public String getVersion() {
    return version;
  }

  /**
   * @return the status
   */
  public int getStatus() {
    return status;
  }

  /**
   * @return the contentType
   */
  public String getContentType() {
    return contentType;
  }

  /**
   * @return the page
   */
  public String getPage() {
    return page;
  }

  public String getFileToDownload() {
    return fileToDownload;
  }

  /**
   * @return the when
   */
  public String getWhen() {
    return when;
  }

  /**
   * @return the tokenAccessor
   */
  public String getTokenAccessor() {
    return tokenAccessor;
  }

  /**
   * @return the data
   */
  public ResultData getData() {
    return data;
  }

  /**
   * @return the debugInfo
   */
  public String getDebugInfo() {
    return debugInfo;
  }

  public boolean isFailedToPersist() {
    return failedToPersist;
  }

  public boolean isLegacy() {
    return isLegacy;
  }

  public static class LegacyData implements ResultData {
    private String header;
    private GfJsonObject content;
    private String footer;

    public LegacyData(String header, GfJsonObject content, String footer) {
      this.header = header;
      this.content = content;
      this.footer = footer;
    }

    public LegacyData(GfJsonObject dataJsonObject) {
      this.header = dataJsonObject.getString("header");
      this.content = dataJsonObject.getJSONObject("content");
      this.footer = dataJsonObject.getString("footer");
    }

    /**
     * @return the header
     */
    public String getHeader() {
      return header;
    }

    /**
     * @return the content
     */
    public Object getContent() {
      return content.getInternalJsonObject();
    }

    /**
     * @return the footer
     */
    public String getFooter() {
      return footer;
    }

    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("Data [header=").append(header).append(", content=").append(content)
          .append(", footer=").append(footer).append("]");
      return builder.toString();
    }
  }

}
