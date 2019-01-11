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

import org.apache.geode.management.internal.cli.json.GfJsonException;
import org.apache.geode.management.internal.cli.json.GfJsonObject;

/**
 * @since GemFire 7.0
 */
public class InfoResultData extends AbstractResultData {
  public static final String RESULT_CONTENT_MESSAGE = "message";

  InfoResultData() {
    super();
  }

  InfoResultData(GfJsonObject gfJsonObject) {
    super(gfJsonObject);
  }

  public InfoResultData(String message) {
    this();
    addLine(message);
  }

  /**
   * @return this InfoResultData
   */
  @Override
  public InfoResultData setHeader(String headerText) {
    return (InfoResultData) super.setHeader(headerText);
  }

  /**
   * @param line message to add
   * @return this InfoResultData
   */
  public InfoResultData addLine(String line) {
    try {
      contentObject.accumulate(RESULT_CONTENT_MESSAGE, line);
    } catch (GfJsonException e) {
      throw new ResultDataException(e.getMessage());
    }

    return this;
  }

  /**
   * @return this InfoResultData
   */
  @Override
  public InfoResultData setFooter(String footerText) {
    return (InfoResultData) super.setFooter(footerText);
  }

  /**
   * @return the gfJsonObject
   */
  @Override
  public GfJsonObject getGfJsonObject() {
    return gfJsonObject;
  }

  @Override
  public String getType() {
    return TYPE_INFO;
  }

  @Override
  public String getHeader() {
    return gfJsonObject.getString(RESULT_HEADER);
  }

  @Override
  public String getFooter() {
    return gfJsonObject.getString(RESULT_FOOTER);
  }
}
