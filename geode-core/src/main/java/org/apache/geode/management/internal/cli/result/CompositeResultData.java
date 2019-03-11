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

import java.util.Iterator;

import org.apache.geode.management.internal.cli.json.GfJsonException;
import org.apache.geode.management.internal.cli.json.GfJsonObject;

/**
 *
 *
 * @since GemFire 7.0
 */
public class CompositeResultData extends AbstractResultData {
  public static final String SEPARATOR = "__separator__";

  private int subsectionCount;

  CompositeResultData() {
    super();
  }

  CompositeResultData(GfJsonObject gfJsonObject) {
    super(gfJsonObject);
  }

  @Override
  public String getType() {
    return TYPE_COMPOSITE;
  }

  /**
   *
   * @return this CompositeResultData
   * @throws ResultDataException If the value is non-finite number or if the key is null.
   */
  @Override
  public CompositeResultData setHeader(String headerText) {
    return (CompositeResultData) super.setHeader(headerText);
  }

  /**
   *
   * @return this CompositeResultData
   * @throws ResultDataException If the value is non-finite number or if the key is null.
   */
  @Override
  public CompositeResultData setFooter(String footerText) {
    return (CompositeResultData) super.setFooter(footerText);
  }

  public SectionResultData addSection() {
    return addSection(String.valueOf(subsectionCount));
  }

  public SectionResultData addSection(String keyToAccess) {
    GfJsonObject sectionData = new GfJsonObject();
    try {
      contentObject.putAsJSONObject(SectionResultData.generateSectionKey(keyToAccess), sectionData);
    } catch (GfJsonException e) {
      throw new ResultDataException(e.getMessage());
    }
    subsectionCount++;
    return new SectionResultData(sectionData);
  }

  public SectionResultData addSection(SectionResultData otherSection) {
    String keyToAccess = String.valueOf(subsectionCount);
    GfJsonObject sectionData = otherSection.getSectionGfJsonObject();
    try {
      contentObject.putAsJSONObject(SectionResultData.generateSectionKey(keyToAccess), sectionData);
    } catch (GfJsonException e) {
      throw new ResultDataException(e.getMessage());
    }
    subsectionCount++;
    return new SectionResultData(sectionData);
  }

  public SectionResultData retrieveSectionByIndex(int index) {
    SectionResultData sectionToRetrieve = null;
    int i = 0;
    for (Iterator<String> iterator = contentObject.keys(); iterator.hasNext();) {
      String key = iterator.next();
      if (key.startsWith(ResultData.SECTION_DATA_ACCESSOR)) {
        if (i == index) {
          sectionToRetrieve = new SectionResultData(contentObject.getJSONObject(key));
          break;
        }
        i++;
      }
    }

    return sectionToRetrieve;
  }

  public SectionResultData retrieveSection(String keyToRetrieve) {
    SectionResultData sectionToRetrieve = null;
    if (contentObject.has(SectionResultData.generateSectionKey(keyToRetrieve))) {
      GfJsonObject sectionData =
          contentObject.getJSONObject(SectionResultData.generateSectionKey(keyToRetrieve));
      sectionToRetrieve = new SectionResultData(sectionData);
    }
    return sectionToRetrieve;
  }

  /**
   *
   * @since GemFire 7.0
   */
  public static class SectionResultData {
    protected GfJsonObject sectionGfJsonObject;

    private int tablesCount = 0;

    SectionResultData() {
      sectionGfJsonObject = new GfJsonObject();
    }

    public SectionResultData(GfJsonObject gfJsonObject) {
      this.sectionGfJsonObject = gfJsonObject;
    }

    public GfJsonObject getSectionGfJsonObject() {
      return sectionGfJsonObject;
    }

    public String getHeader() {
      return sectionGfJsonObject.getString(RESULT_HEADER);
    }

    public String getFooter() {
      return sectionGfJsonObject.getString(RESULT_FOOTER);
    }

    public SectionResultData setHeader(String headerText) {
      try {
        sectionGfJsonObject.put(RESULT_HEADER, headerText);
      } catch (GfJsonException e) {
        throw new ResultDataException(e.getMessage());
      }

      return this;
    }

    public SectionResultData setFooter(String footerText) {
      try {
        sectionGfJsonObject.put(RESULT_FOOTER, footerText);
      } catch (GfJsonException e) {
        throw new ResultDataException(e.getMessage());
      }

      return this;
    }

    public SectionResultData addSeparator(char buildSeparatorFrom) {
      try {
        sectionGfJsonObject.put(SEPARATOR, buildSeparatorFrom);
      } catch (GfJsonException e) {
        throw new ResultDataException(e.getMessage());
      }

      return this;
    }

    public SectionResultData addData(String name, Object value) {
      try {
        sectionGfJsonObject.put(name, value);
      } catch (GfJsonException e) {
        throw new ResultDataException(e.getMessage());
      }
      return this;
    }

    public TabularResultData addTable() {
      return addTable(String.valueOf(tablesCount));
    }

    public TabularResultData addTable(String keyToAccess) {
      TabularResultData tabularResultData = ResultBuilder.createTabularResultData();
      GfJsonObject tableData = tabularResultData.getGfJsonObject();
      try {
        sectionGfJsonObject.putAsJSONObject(generateTableKey(keyToAccess), tableData);
      } catch (GfJsonException e) {
        throw new ResultDataException(e.getMessage());
      }
      tablesCount++;
      return tabularResultData;
    }

    public TabularResultData retrieveTableByIndex(int index) {
      TabularResultData tabularResultData = null;
      int i = 0;
      for (Iterator<String> iterator = sectionGfJsonObject.keys(); iterator.hasNext();) {
        String key = iterator.next();
        if (key.startsWith(ResultData.TABLE_DATA_ACCESSOR)) {
          if (i == index) {
            tabularResultData = new TabularResultData(sectionGfJsonObject.getJSONObject(key));
            break;
          }
          i++;
        }
      }

      return tabularResultData;
    }

    public TabularResultData retrieveTable(String keyToRetrieve) {
      TabularResultData tabularResultData = null;
      if (sectionGfJsonObject.has(generateTableKey(keyToRetrieve))) {
        GfJsonObject tableData = sectionGfJsonObject.getJSONObject(generateTableKey(keyToRetrieve));
        tabularResultData = new TabularResultData(tableData);
      }
      return tabularResultData;
    }

    public static String generateSectionKey(String keyToRetrieve) {
      return SECTION_DATA_ACCESSOR + "-" + keyToRetrieve;
    }

    public static String generateTableKey(String keyToRetrieve) {
      return TABLE_DATA_ACCESSOR + "-" + keyToRetrieve;
    }
  }

}
