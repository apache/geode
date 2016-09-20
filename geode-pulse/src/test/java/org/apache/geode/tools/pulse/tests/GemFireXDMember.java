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

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;

public class GemFireXDMember extends JMXBaseBean implements
    GemFireXDMemberMBean {
  private String name = null;

  private static String[] itemNames = { "connectionsAttempted",
      "connectionsActive", "connectionsClosed", "connectionsFailed" };;
  private static String[] itemDescriptions = { "connectionsAttempted",
      "connectionsActive", "connectionsClosed", "connectionsFailed" };
  private static OpenType[] itemTypes = { SimpleType.LONG, SimpleType.LONG,
      SimpleType.LONG, SimpleType.LONG };
  private static CompositeType networkServerClientConnectionStats = null;

  static {
    try {
      networkServerClientConnectionStats = new CompositeType(
          "NetworkServerClientConnectionStats",
          "Network Server Client Connection Stats Information", itemNames,
          itemDescriptions, itemTypes);

    } catch (OpenDataException e) {
      e.printStackTrace();
    }
  }

  public GemFireXDMember(String name) {
    this.name = name;
  }

  @Override
  protected String getKey(String propName) {
    return "gemfirexdmember." + name + "." + propName;
  }

  @Override
  public boolean getDataStore() {
    return getBoolean("DataStore");
  }

  @Override
  public CompositeData getNetworkServerClientConnectionStats() {
    Long[] itemValues = getLongArray("NetworkServerClientConnectionStats");
    CompositeData nscCompData;
    try {
      nscCompData = new CompositeDataSupport(
          networkServerClientConnectionStats, itemNames, itemValues);
    } catch (OpenDataException e) {
      e.printStackTrace();
      nscCompData = null;
    }
    return nscCompData;
  }

}
