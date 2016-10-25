/*
 *
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
 *
 */
package org.apache.geode.tools.pulse.tests;

import java.util.HashMap;
import java.util.Map;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;

public class Region extends JMXBaseBean implements RegionMBean {
  private String name = null;

  private static String[] regAttItemNames = { "compressionCodec",
    "enableOffHeapMemory", "scope", "diskStoreName",
    "diskSynchronous" };
  private static String[] regAttItemDescriptions = { "compressionCodec",
    "enableOffHeapMemory", "scope", "diskStoreName",
    "diskSynchronous" };
  private static OpenType[] regAttItemTypes = { SimpleType.STRING,
    SimpleType.BOOLEAN, SimpleType.STRING,
    SimpleType.STRING, SimpleType.BOOLEAN };
  private static CompositeType listRegionAttributesCompData = null;

  static {
    try {
      listRegionAttributesCompData = new CompositeType("listRegionAttributes",
          "Regions attributes", regAttItemNames, regAttItemDescriptions,
          regAttItemTypes);

    } catch (OpenDataException e) {
      e.printStackTrace();
    }
  }

  public Region(String name) {
    this.name = name;
  }

  protected String getKey(String propName) {
    return "region." + name + "." + propName;
  }

  @Override
  public String[] getMembers() {
    return getStringArray("members");
  }

  @Override
  public String getFullPath() {
    return getString("fullPath");
  }

  @Override
  public double getDiskReadsRate() {
    return getDouble("diskReadsRate");
  }

  @Override
  public double getDiskWritesRate() {
    return getDouble("diskWritesRate");
  }

  @Override
  public int getEmptyNodes() {
    return getInt("emptyNodes");
  }

  @Override
  public double getGetsRate() {
    return getDouble("getsRate");
  }

  @Override
  public double getLruEvictionRate() {
    return getDouble("lruEvictionRate");
  }

  @Override
  public double getPutsRate() {
    return getDouble("putsRate");
  }

  @Override
  public String getRegionType() {
    return getString("regionType");
  }

  @Override
  public long getEntrySize() {
    return getLong("entrySize");
  }

  @Override
  public long getSystemRegionEntryCount() {
    return getLong("systemRegionEntryCount");
  }

  @Override
  public int getMemberCount() {
    return getInt("memberCount");
  }

  @Override
  public boolean getPersistentEnabled() {
    return getBoolean("persistentEnabled");
  }

  @Override
  public String getName() {
    return getString("name");
  }

  @Override
  public boolean getGatewayEnabled() {
    return getBoolean("gatewayEnabled");
  }

  @Override
  public long getDiskUsage() {
    return getLong("diskUsage");
  }

  @Override
  public CompositeData listRegionAttributes() {
    String value = JMXProperties.getInstance().getProperty(
        getKey("listRegionAttributes"), "");
    String[] itemValues = value.split(",");
    Map<String, Object> itemValuesHM = new HashMap<String, Object>();
    
    // compressionCodec
    if (null != itemValues[0]) {
      itemValuesHM.put(regAttItemNames[0], itemValues[0]);
    }

    // enableOffHeapMemory
    if (null != itemValues[1]) {
      itemValuesHM.put(regAttItemNames[1], Boolean.parseBoolean(itemValues[1]));
    }

    // scope
    if (null != itemValues[3]) {
      itemValuesHM.put(regAttItemNames[3], itemValues[3]);
    }

    // diskStoreName
    if (null != itemValues[4]) {
      itemValuesHM.put(regAttItemNames[4], itemValues[4]);
    }

    // diskSynchronous
    if (null != itemValues[5]) {
      itemValuesHM.put(regAttItemNames[5], Boolean.parseBoolean(itemValues[5]));
    }

    CompositeData lraCompData;
    try {
      lraCompData = new CompositeDataSupport(listRegionAttributesCompData,
          itemValuesHM);
    } catch (OpenDataException e) {
      e.printStackTrace();
      lraCompData = null;
    }
    return lraCompData;
  }

}
