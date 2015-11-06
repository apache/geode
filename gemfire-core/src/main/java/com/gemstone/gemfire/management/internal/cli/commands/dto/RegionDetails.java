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
package com.gemstone.gemfire.management.internal.cli.commands.dto;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.management.internal.cli.json.GfJsonArray;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonException;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonObject;
import com.gemstone.gemfire.management.internal.cli.result.CliJsonSerializable;
import com.gemstone.gemfire.management.internal.cli.util.JsonUtil;

public class RegionDetails implements CliJsonSerializable {
  private static Map<String, String> nameToDisplayName = new HashMap<String, String>();
  
  static {
    nameToDisplayName.put("name",        "Name");
    nameToDisplayName.put("path",        "Path");
    nameToDisplayName.put("partitioned", "Is Partitioned");
    nameToDisplayName.put("persistent",  "Is Persistent");
    nameToDisplayName.put("groups",      "Group(s)");
    nameToDisplayName.put("regionAttributesInfo",    "Region Attributes");
    nameToDisplayName.put("regionMemberDetailsList", "On Members");
  }
  
  private String   name;
  private String   path;
  private boolean  isPartitioned;
  private boolean  isPersistent;
  private String[] groups;
  
  private RegionAttributesInfo regionAttributesInfo;
  
  private List<RegionMemberDetails> regionMemberDetailsList;
  
  private String[] fieldsToSkipOnUI;   
  
  public String getName() {
    return name;
  }
  public void setName(String name) {
    this.name = name;
  }
  public String getPath() {
    return path;
  }
  public void setPath(String path) {
    this.path = path;
  }
  public boolean isPartitioned() {
    return isPartitioned;
  }
  public void setPartitioned(boolean isPartitioned) {
    this.isPartitioned = isPartitioned;
  }
  public boolean isPersistent() {
    return isPersistent;
  }
  public void setPersistent(boolean isPersistent) {
    this.isPersistent = isPersistent;
  }
  public RegionAttributesInfo getRegionAttributesInfo() {
    return regionAttributesInfo;
  }
  public void setRegionAttributesInfo(RegionAttributesInfo regionAttributesInfo) {
    this.regionAttributesInfo = regionAttributesInfo;
  }
  public List<RegionMemberDetails> getRegionMemberDetailsList() {
    return regionMemberDetailsList;
  }
  public void setRegionMemberDetailsList(List<RegionMemberDetails> regionMemberDetailsList) {
    this.regionMemberDetailsList = regionMemberDetailsList;
  }
  public String[] getGroups() {
    return groups;
  }
  public void setGroups(String ... groups) {
    this.groups = groups;
  }
  
  
  @Override
  public int getJSId() {
    return CLI_DOMAIN_OBJECT__REGION_DETAILS;
  }
  @Override
  public Map<String, String> getFieldNameToDisplayName() {
    return nameToDisplayName;
  }

  @Override
  public String[] getFieldsToSkipOnUI() {
    return fieldsToSkipOnUI;
  }
  
  @Override
  public void setFieldsToSkipOnUI(String ... fieldsToSkipOnUI) {
    this.fieldsToSkipOnUI = fieldsToSkipOnUI;
  }
  
  @Override
  public void fromJson(GfJsonObject objectStateAsjson) {
    this.name = JsonUtil.getString(objectStateAsjson, "name");
    this.path =  JsonUtil.getString(objectStateAsjson, "path");
    this.isPartitioned = JsonUtil.getBoolean(objectStateAsjson, "partitioned"); // NOTE: no 'is' in names
    this.isPersistent = JsonUtil.getBoolean(objectStateAsjson, "persistent");
    this.groups = JsonUtil.getStringArray(objectStateAsjson, "groups");

    if (objectStateAsjson.has("regionAttributesInfo")) {
      this.regionAttributesInfo = new RegionAttributesInfo();
      this.regionAttributesInfo.fromJson(JsonUtil.getJSONObject(objectStateAsjson, "regionAttributesInfo"));
    }
    
    List<CliJsonSerializable> retrievedList = JsonUtil.getList(objectStateAsjson, "regionMemberDetailsList");
    regionMemberDetailsList = new ArrayList<RegionMemberDetails>();
    for (int i = 0; i < retrievedList.size(); i++) { // What's the better way? 
      regionMemberDetailsList.add((RegionMemberDetails) retrievedList.get(i));
    }
  }
  
  @Override
  public int hashCode() { // eclipse generated
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(fieldsToSkipOnUI);
    result = prime * result + Arrays.hashCode(groups);
    result = prime * result + (isPartitioned ? 1231 : 1237);
    result = prime * result + (isPersistent ? 1231 : 1237);
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    result = prime * result + ((path == null) ? 0 : path.hashCode());
    result = prime
        * result
        + ((regionAttributesInfo == null) ? 0 : regionAttributesInfo.hashCode());
    result = prime
        * result
        + ((regionMemberDetailsList == null) ? 0 : regionMemberDetailsList
            .hashCode());
    return result;
  }
  @Override
  public boolean equals(Object obj) { // eclipse generated
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    RegionDetails other = (RegionDetails) obj;
    if (!Arrays.equals(fieldsToSkipOnUI, other.fieldsToSkipOnUI))
      return false;
    if (!Arrays.equals(groups, other.groups))
      return false;
    if (isPartitioned != other.isPartitioned)
      return false;
    if (isPersistent != other.isPersistent)
      return false;
    if (name == null) {
      if (other.name != null)
        return false;
    } else if (!name.equals(other.name))
      return false;
    if (path == null) {
      if (other.path != null)
        return false;
    } else if (!path.equals(other.path))
      return false;
    if (regionAttributesInfo == null) {
      if (other.regionAttributesInfo != null)
        return false;
    } else if (!regionAttributesInfo.equals(other.regionAttributesInfo))
      return false;
    if (regionMemberDetailsList == null) {
      if (other.regionMemberDetailsList != null)
        return false;
    } else if (!areMemberDetailsSame(regionMemberDetailsList, other.regionMemberDetailsList))
      return false;
    return true;
  }
  
  private boolean areMemberDetailsSame(List<RegionMemberDetails> mine, List<RegionMemberDetails> other) {
    if (other != null && other.size() == mine.size()) {    
      for (int i = 0; i < mine.size(); i++) {
        if (! mine.get(i).equals(other.get(i))) {
          return false;
        }
      }
    } else {
      return false;
    }    
    return true;
  }
  
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("RegionDetails [name=");
    builder.append(name);
    builder.append(", path=");
    builder.append(path);
    builder.append(", isPartitioned=");
    builder.append(isPartitioned);
    builder.append(", isPersistent=");
    builder.append(isPersistent);
    builder.append(", groups=");
    builder.append(Arrays.toString(groups));
    builder.append(", regionAttributesInfo=");
    builder.append(regionAttributesInfo);
    builder.append(", regionMemberDetailsList=");
    builder.append(regionMemberDetailsList);
    builder.append("]");
    return builder.toString();
  }
}
