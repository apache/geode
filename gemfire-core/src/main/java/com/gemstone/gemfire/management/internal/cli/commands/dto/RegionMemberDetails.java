/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.commands.dto;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.gemstone.gemfire.management.internal.cli.json.GfJsonArray;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonException;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonObject;
import com.gemstone.gemfire.management.internal.cli.result.CliJsonSerializable;
import com.gemstone.gemfire.management.internal.cli.util.JsonUtil;

public class RegionMemberDetails  implements CliJsonSerializable{
  private static Map<String, String> nameToDisplayName = new HashMap<String, String>();
  
  static {
    nameToDisplayName.put("id",                "Member Id");
    nameToDisplayName.put("primaryEntryCount", "PrimaryEntryCount");
    nameToDisplayName.put("backupEntryCount",  "BbackupEntryCount");
    nameToDisplayName.put("memory",            "Memory");
    nameToDisplayName.put("numOfCopies",       "NumOfCopies");
    nameToDisplayName.put("numOfBuckets",      "NumOfBuckets");
  }
  
  private String id;
  private long   primaryEntryCount;
  private long   backupEntryCount;
  private String memory;
  private int    numOfCopies;
  private int    numOfBuckets;
  
  private String[] fieldsToSkipOnUI;
  
  public RegionMemberDetails() {
  }
  
  public RegionMemberDetails(String id, long primaryEntryCount,
      long backupEntryCount, String memory, int numOfCopies, int numOfBuckets) {
    this.id = id;
    this.primaryEntryCount = primaryEntryCount;
    this.backupEntryCount = backupEntryCount;
    this.memory = memory;
    this.numOfCopies = numOfCopies;
    this.numOfBuckets = numOfBuckets;
  }
  
  public String getId() {
    return id;
  }
  public void setId(String id) {
    this.id = id;
  }
  public long getPrimaryEntryCount() {
    return primaryEntryCount;
  }
  public void setPrimaryEntryCount(long primaryEntryCount) {
    this.primaryEntryCount = primaryEntryCount;
  }
  public long getBackupEntryCount() {
    return backupEntryCount;
  }
  public void setBackupEntryCount(long backupEntryCount) {
    this.backupEntryCount = backupEntryCount;
  }
  public String getMemory() {
    return memory;
  }
  public void setMemory(String memory) {
    this.memory = memory;
  }
  public int getNumOfCopies() {
    return numOfCopies;
  }
  public void setNumOfCopies(int numOfCopies) {
    this.numOfCopies = numOfCopies;
  }
  public int getNumOfBuckets() {
    return numOfBuckets;
  }
  public void setNumOfBuckets(int numOfBuckets) {
    this.numOfBuckets = numOfBuckets;
  }

  @Override
  public int getJSId() {
    return CLI_DOMAIN_OBJECT__REGION_MEMBER_DETAILS;
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
    this.id = JsonUtil.getString(objectStateAsjson, "id");
    this.primaryEntryCount = JsonUtil.getLong(objectStateAsjson, "primaryEntryCount");
    this.backupEntryCount = JsonUtil.getLong(objectStateAsjson, "backupEntryCount");
    this.memory = JsonUtil.getString(objectStateAsjson, "memory");
    this.numOfCopies = JsonUtil.getInt(objectStateAsjson, "numOfCopies");
    this.numOfBuckets = JsonUtil.getInt(objectStateAsjson, "numOfBuckets");
    this.fieldsToSkipOnUI = JsonUtil.getStringArray(objectStateAsjson, "fieldsToSkipOnUI");
  }
  
  

  @Override
  public int hashCode() { // eclipse generated
    final int prime = 31;
    int result = 1;
    result = prime * result
        + (int) (backupEntryCount ^ (backupEntryCount >>> 32));
    result = prime * result + Arrays.hashCode(fieldsToSkipOnUI);
    result = prime * result + ((id == null) ? 0 : id.hashCode());
    result = prime * result + ((memory == null) ? 0 : memory.hashCode());
    result = prime * result + numOfBuckets;
    result = prime * result + numOfCopies;
    result = prime * result
        + (int) (primaryEntryCount ^ (primaryEntryCount >>> 32));
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
    RegionMemberDetails other = (RegionMemberDetails) obj;
    if (backupEntryCount != other.backupEntryCount)
      return false;
    if (!Arrays.equals(fieldsToSkipOnUI, other.fieldsToSkipOnUI))
      return false;
    if (id == null) {
      if (other.id != null)
        return false;
    } else if (!id.equals(other.id))
      return false;
    if (memory == null) {
      if (other.memory != null)
        return false;
    } else if (!memory.equals(other.memory))
      return false;
    if (numOfBuckets != other.numOfBuckets)
      return false;
    if (numOfCopies != other.numOfCopies)
      return false;
    if (primaryEntryCount != other.primaryEntryCount)
      return false;
    return true;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("RegionMemberDetails [id=");
    builder.append(id);
    builder.append(", primaryEntryCount=");
    builder.append(primaryEntryCount);
    builder.append(", backupEntryCount=");
    builder.append(backupEntryCount);
    builder.append(", memory=");
    builder.append(memory);
    builder.append(", numOfCopies=");
    builder.append(numOfCopies);
    builder.append(", numOfBuckets=");
    builder.append(numOfBuckets);
    builder.append("]");
    return builder.toString();
  }
}
