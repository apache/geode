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

import java.util.ArrayList;
import java.util.List;

import org.apache.geode.management.internal.cli.json.GfJsonArray;
import org.apache.geode.management.internal.cli.json.GfJsonException;
import org.apache.geode.management.internal.cli.json.GfJsonObject;

/**
 * @since GemFire 7.0
 */
public class TabularResultData extends AbstractResultData {
  /* package */ TabularResultData() {
    super();
  }

  /* package */ TabularResultData(GfJsonObject gfJsonObject) {
    super(gfJsonObject);
  }

  public TabularResultData accumulate(String accumulateFor, Object value) {
    try {
      contentObject.accumulate(accumulateFor, value);
    } catch (GfJsonException e) {
      throw new ResultDataException(e.getMessage());
    }
    return this;
  }

  public GfJsonArray getHeaders() {
    try {
      return this.contentObject.names();
    } catch (GfJsonException e) {
      e.printStackTrace();
    }
    return null;
  }

  public int columnSize() {
    return contentObject.size();
  }

  public int rowSize(String key) {
    GfJsonArray jsonArray = null;
    try {
      jsonArray = contentObject.getJSONArray(key);
    } catch (GfJsonException e) {
      throw new RuntimeException("unable to get the row size of " + key);
    }
    if (jsonArray == null) {
      return 0;
    }

    return jsonArray.getInternalJsonArray().length();
  }

  /**
   * @return the gfJsonObject
   */
  public GfJsonObject getGfJsonObject() {
    return gfJsonObject;
  }

  @Override
  public String getType() {
    return TYPE_TABULAR;
  }

  /**
   * @param headerText Text to set to header.
   * @return this TabularResultData
   * @throws ResultDataException If the value is non-finite number or if the key is null.
   */
  public TabularResultData setHeader(String headerText) {
    return (TabularResultData) super.setHeader(headerText);
  }

  /**
   * @param footerText Text to set to footer.
   * @return this TabularResultData
   * @throws ResultDataException If the value is non-finite number or if the key is null.
   */
  public TabularResultData setFooter(String footerText) {
    return (TabularResultData) super.setFooter(footerText);
  }

  @Override
  public String getHeader() {
    return gfJsonObject.getString(RESULT_HEADER);
  }

  @Override
  public String getFooter() {
    return gfJsonObject.getString(RESULT_FOOTER);
  }

  public List<String> retrieveAllValues(String columnName) {
    List<String> values = new ArrayList<>();

    try {
      GfJsonArray jsonArray = contentObject.getJSONArray(columnName);
      int size = jsonArray.size();
      for (int i = 0; i < size; i++) {
        values.add(String.valueOf(jsonArray.get(i)));
      }
    } catch (GfJsonException e) {
      throw new ResultDataException(e.getMessage());
    }
    return values;
  }
}
