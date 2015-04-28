/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli;

import java.text.DateFormat;

import com.gemstone.gemfire.internal.GemFireVersion;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonObject;

/**
 * 
 * @author Abhishek Chaudhari
 * @since 7.0
 */
public class CommandResponse {
  
  private final String sender;
  private final String version;
  private final int    status;
  private final String contentType;
  private final String page;
  private final String when;
  private final String tokenAccessor;
  private final String debugInfo;
  private final Data   data;
  private final boolean failedToPersist;
  
  CommandResponse(String sender, String contentType, int status, 
      String page, String tokenAccessor, String debugInfo, String header, 
      GfJsonObject content, String footer, boolean failedToPersist) {
    this.sender        = sender;
    this.contentType   = contentType;
    this.status        = status;
    this.page          = page;
    this.tokenAccessor = tokenAccessor;
    this.debugInfo     = debugInfo;
    this.data          = new Data(header, content, footer);
    this.when          = DateFormat.getInstance().format(new java.util.Date());
    this.version       = GemFireVersion.getGemFireVersion();
    this.failedToPersist = failedToPersist;
  }
  
  // For de-serializing
  CommandResponse(GfJsonObject jsonObject) {
    this.sender        = jsonObject.getString("sender");
    this.contentType   = jsonObject.getString("contentType");
    this.status        = jsonObject.getInt("status");
    this.page          = jsonObject.getString("page");
    this.tokenAccessor = jsonObject.getString("tokenAccessor");
    this.debugInfo     = jsonObject.getString("debugInfo");
    this.data          = new Data(jsonObject.getJSONObject("data"));
    this.when          = jsonObject.getString("when");
    this.version       = jsonObject.getString("version");
    this.failedToPersist = jsonObject.getBoolean("failedToPersist");
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
  public Data getData() {
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

  public static class Data {
    private String       header;
    private GfJsonObject content;
    private String       footer;
    
    public Data(String header, GfJsonObject content, String footer) {
      this.header  = header;
      this.content = content;
      this.footer  = footer;
    }
    
    public Data(GfJsonObject dataJsonObject) {
      this.header  = dataJsonObject.getString("header");
      this.content = dataJsonObject.getJSONObject("content");
      this.footer  = dataJsonObject.getString("footer");
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
      builder.append("Data [header=").append(header)
             .append(", content=").append(content)
             .append(", footer=").append(footer).append("]");
      return builder.toString();
    }
  }
}


/*
** TABLE

{
  "sender": "member1",
  "version": "gemfire70",
  "contentType": "table",
  "page": "1/1",
  "tokenAccessor": "__NULL__",
  "status": "OK",
  "when": "January 12 2012",
  "debugData": [
    "val1",
    "val2"
  ],
  "data": {
    "header": [
      "Header1",
      "Header2",
      "Header3",
      "Header4"
    ],
    "content": [
      [
        "val00",
        "val01",
        "val02",
        "val03"
      ],
      [
        "val10",
        "val11",
        "val12",
        "val13"
      ],
      [
        "val20",
        "val21",
        "val22",
        "val23"
      ]
    ]
  }
}

** TABLE SCROLLABLE

{
  "sender": "member1",
  "version": "gemfire70",
  "contentType": "table",
  "page": "1/5",
  "tokenHolder": "TOKEN12345",
  "status": "OK",
  "when": "January 12 2012",
  "debugData": [
    "val1",
    "val2"
  ],
  "data": {
    "header": [
      "Header1",
      "Header2",
      "Header3",
      "Header4"
    ],
    "content": [
      [
        "val00",
        "val01",
        "val02",
        "val03"
      ],
      [
        "val10",
        "val11",
        "val12",
        "val13"
      ],
      [
        "val20",
        "val21",
        "val22",
        "val23"
      ]
    ]
  }
}


** CATALOG

{
  "sender": "member1",
  "version": "gemfire70",
  "contentType": "catalog",
  "page": "1/1",
  "tokenHolder": "__NULL__",
  "status": "OK",
  "when": "January 12 2012",
  "debugData": [
    "val1",
    "val2"
  ],
  "data": {
    "content": [
      {
        "key1": "val1",
        "key2": "val2",
        "key3": "val3",
        "key4": "val4",
        "key5": "val5",
        "key6": "val6",
        "key7": "val7"
      }
    ]
  }
}


** CATALOG SCROLLABLE

{
  "sender": "member1",
  "version": "gemfire70",
  "contentType": "catalog",
  "page": "1/10",
  "tokenHolder": "TOKEN1265765",
  "status": "OK",
  "when": "January 12 2012",
  "debugData": [
    "val1",
    "val2"
  ],
  "data": {
    "content": [
      {
        "key1": "val1",
        "key2": "val2",
        "key3": "val3",
        "key4": "val4",
        "key5": "val5",
        "key6": "val6",
        "key7": "val7"
      }
    ]
  }
}


** Object as argument

{
  "com.foo.bar.Employee": {
    "id": 1234,
    "name": "Foo BAR",
    "department": {
      "id": 456,
      "name": "support"
    }
  }
}

*/