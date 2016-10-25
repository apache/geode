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

/**
 * Region on member mbean
 *
 *
 *
 */
public class RegionOnMember extends JMXBaseBean implements RegionOnMemberMBean {
  private String fullPath = null;
  private String member = null;

  public RegionOnMember(String fullPath, String member) {
    this.fullPath = fullPath;
    this.member = member;
  }

  @Override
  protected String getKey(String propName) {
    return "regionOnMember." + fullPath + "." + member + "." + propName;
  }

  @Override
  public String getFullPath(){
    return this.fullPath;
  }

  @Override
  public String getMember(){
    return this.member;
  }

  @Override
  public String getName(){
    return getString("name");
  }

  @Override
  public String getRegionType(){
    return getString("regionType");
  }

  @Override
  public long getEntrySize(){
    return getLong("entrySize");
  }

  @Override
  public long getEntryCount(){
    return getLong("entryCount");
  }

  @Override
  public double getGetsRate(){
    return getDouble("getsRate");
  }

  @Override
  public double getPutsRate(){
    return getDouble("putsRate");
  }

  @Override
  public double getDiskReadsRate(){
    return getDouble("diskGetsRate");
  }

  @Override
  public double getDiskWritesRate(){
    return getDouble("diskPutsRate");
  }

  @Override
  public int getLocalMaxMemory(){
    return getInt("localMaxMemory");
  }
}
