/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.management.internal.cli.result;

import java.util.Iterator;

import com.gemstone.gemfire.management.internal.cli.json.GfJsonArray;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonException;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonObject;

/**
 * 
 * 
 * @since GemFire 7.0
 */
public class CompositeResultData extends AbstractResultData {
  public static String SEPARATOR = "__separator__";
  
  private int subsectionCount;

  /*package*/CompositeResultData() {
    super();
  }
  
  /*package*/CompositeResultData(GfJsonObject gfJsonObject) {
    super(gfJsonObject);
  }

  @Override
  public String getType() {
    return TYPE_COMPOSITE;
  }
  
  /**
   * 
   * @param headerText
   * @return this CompositeResultData
   * @throws ResultDataException
   *           If the value is non-finite number or if the key is null.
   */
  public CompositeResultData setHeader(String headerText) {
    return (CompositeResultData) super.setHeader(headerText);
  }
  
  /**
   * 
   * @param footerText
   * @return this CompositeResultData
   * @throws ResultDataException
   *           If the value is non-finite number or if the key is null.
   */
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
  
  public CompositeResultData addSeparator(char buildSeparatorFrom) {
    try {
      contentObject.put(SEPARATOR, buildSeparatorFrom);
    } catch (GfJsonException e) {
      throw new ResultDataException(e.getMessage());
    }
    
    return this;
  }
  
  public SectionResultData retrieveSectionByIndex(int index) {
    SectionResultData sectionToRetrieve = null;
    int i = 0;
    for (Iterator<String> iterator = contentObject.keys(); iterator.hasNext();) {
      String key = iterator.next();
      if (key.startsWith(CompositeResultData.SECTION_DATA_ACCESSOR)) {
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
      GfJsonObject sectionData = contentObject.getJSONObject(SectionResultData.generateSectionKey(keyToRetrieve));
      sectionToRetrieve = new SectionResultData(sectionData);
    }
    return sectionToRetrieve;
  }
  
  /**
   * 
   * @since GemFire 7.0
   */
  public static class SectionResultData /*extends AbstractResultData*/ {
    protected GfJsonObject sectionGfJsonObject;
    
    private int subsectionCount = 0;
    private int tablesCount     = 0;
    
    /*package*/SectionResultData() {
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
    
    public SectionResultData addSection() {
      return addSection(String.valueOf(subsectionCount));
    }
    
    public SectionResultData addSection(String keyToAccess) {
      GfJsonObject sectionData = new GfJsonObject();
      try {
        sectionGfJsonObject.putAsJSONObject(generateSectionKey(keyToAccess), sectionData);
      } catch (GfJsonException e) {
        throw new ResultDataException(e.getMessage());
      }
      subsectionCount++;
      return new SectionResultData(sectionData);
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
    
    public SectionResultData retrieveSectionByIndex(int index) {
      SectionResultData sectionToRetrieve = null;
      int i = 0;
      for (Iterator<String> iterator = sectionGfJsonObject.keys(); iterator.hasNext();) {
        String key = iterator.next();
        if (key.startsWith(CompositeResultData.SECTION_DATA_ACCESSOR)) {
          if (i == index) {
            sectionToRetrieve = new SectionResultData(sectionGfJsonObject.getJSONObject(key));
            break;
          }
          i++;
        }
      }
      
      return sectionToRetrieve;
    }
    
    public SectionResultData retrieveSection(String keyToRetrieve) {
      SectionResultData sectionToRetrieve = null;
      if (sectionGfJsonObject.has(generateSectionKey(keyToRetrieve))) {
        GfJsonObject sectionData = sectionGfJsonObject.getJSONObject(generateSectionKey(keyToRetrieve));
        sectionToRetrieve = new SectionResultData(sectionData);
      }
      return sectionToRetrieve;
    }
    
    public TabularResultData retrieveTableByIndex(int index) {
      TabularResultData tabularResultData = null;
      int i = 0;
      for (Iterator<String> iterator = sectionGfJsonObject.keys(); iterator.hasNext();) {
        String key = iterator.next();
        if (key.startsWith(CompositeResultData.TABLE_DATA_ACCESSOR)) {
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
    
    public Object retrieveObject(String name) {
      Object retrievedObject = sectionGfJsonObject.get(name);
      
      if (retrievedObject instanceof GfJsonArray) {
        GfJsonArray.toStringArray(((GfJsonArray)retrievedObject));
      }
      
      return sectionGfJsonObject.get(name);
    }
    
    public String retrieveString(String name) {
      return sectionGfJsonObject.getString(name);
    }
    
    public String[] retrieveStringArray(String name) {
      String[] stringArray = null;
      Object retrievedObject = sectionGfJsonObject.get(name);
      
      if (retrievedObject instanceof GfJsonArray) {
        stringArray = GfJsonArray.toStringArray(((GfJsonArray)retrievedObject));
      } else {
        try {
          stringArray = GfJsonArray.toStringArray(new GfJsonArray(retrievedObject));
        } catch (GfJsonException e) {
          throw new ResultDataException(e.getMessage());
        }
      }
      return stringArray;
    }
    
    public static String generateSectionKey(String keyToRetrieve) {
      return SECTION_DATA_ACCESSOR + "-" + keyToRetrieve;
    }
    
    public static String generateTableKey(String keyToRetrieve) {
      return TABLE_DATA_ACCESSOR + "-" + keyToRetrieve;
    }

//    @Override
//    public String getType() {
//      return TYPE_SECTION;
//    }
  }
  
  public static void main(String[] args) {
    CompositeResultData crd = new CompositeResultData();
    
    SectionResultData r1Section = crd.addSection("R1");
    r1Section.addData("Region", "R1").addData("IsPartitioned", false).addData("IsPersistent", true).addData("Disk Store", "DiskStore1").addData("Group", "Group1");
    TabularResultData r1Table = r1Section.addTable("R1Members");
    r1Table.accumulate("Member Id", "host1(3467):12435:12423").accumulate("PrimaryEntryCount", 20000).accumulate("BackupEntryCount", 20000).accumulate("Memory(MB)", "100").accumulate("NumOfCopies", 1);
    r1Table.accumulate("Member Id", "host3(5756):57665:90923").accumulate("PrimaryEntryCount", 25000).accumulate("BackupEntryCount", 10000).accumulate("Memory(MB)", "200").accumulate("NumOfCopies", 1);
    
    SectionResultData r3Section = crd.addSection("R3");
    r3Section.addData("Region", "R3").addData("IsPartitioned", true).addData("IsPersistent", true).addData("Disk Store", "DiskStore2").addData("Group", "Group2").addData("ColocatedWith", "-");
    SectionResultData r3SubSection = r3Section.addSection("R3Config");
    r3SubSection.addData("Configuration", "");
    r3SubSection.addData("Config1", "abcd");
    r3SubSection.addData("Config2", "abcde");
    r3SubSection.addData("Config3", "abcdfg");
    TabularResultData r3Table = r3Section.addTable("R3Members");
    r3Table.accumulate("Member Id", "host1(3467):12435:12423").accumulate("PrimaryEntryCount", 20000).accumulate("BackupEntryCount", 20000).accumulate("Memory(MB)", "100").accumulate("NumOfCopies", 1).accumulate("NumOfBuckets", 100);
    r3Table.accumulate("Member Id", "host2(3353):23545:14723").accumulate("PrimaryEntryCount", 20000).accumulate("BackupEntryCount", 20000).accumulate("Memory(MB)", "100").accumulate("NumOfCopies", 1).accumulate("NumOfBuckets", 100);
    r3Table.accumulate("Member Id", "host3(5756):57665:90923").accumulate("PrimaryEntryCount", 25000).accumulate("BackupEntryCount", 10000).accumulate("Memory(MB)", "200").accumulate("NumOfCopies", 1).accumulate("NumOfBuckets", 100);

    try {
      System.out.println(crd.getGfJsonObject().toIndentedString(/*2*/0));
      
    } catch (GfJsonException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
