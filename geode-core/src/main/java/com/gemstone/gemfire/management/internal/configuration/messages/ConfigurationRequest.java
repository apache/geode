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
package com.gemstone.gemfire.management.internal.configuration.messages;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.lang.StringUtils;

/***
 * Request sent by a member to the locator requesting the shared configuration
 *
 */
public class ConfigurationRequest implements DataSerializableFixedID{
  private static int DEFAULT_NUM_ATTEMPTS = 5;
  private Set<String> groups = new HashSet<String>();
  private boolean isRequestForEntireConfiguration = false;
  private int numAttempts = DEFAULT_NUM_ATTEMPTS; 
  
  public ConfigurationRequest() {
    super();
  }
  
  public ConfigurationRequest(Set<String> groups) {
    this.groups = groups;
    this.isRequestForEntireConfiguration = false;
  }
  
  public ConfigurationRequest(boolean getEntireConfiguration) {
    this.isRequestForEntireConfiguration = true;
  }
  
  public void addGroups(String group) {
    if (!StringUtils.isBlank(group))
      this.groups.add(group);
  }
  
  @Override
  public int getDSFID() {
    return DataSerializableFixedID.CONFIGURATION_REQUEST;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    out.writeBoolean(isRequestForEntireConfiguration);
    int size = groups.size();
    out.writeInt(size);
    if (size > 0) {
      for(String group : groups) {
        out.writeUTF(group);
      }
    }
    out.writeInt(numAttempts);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.isRequestForEntireConfiguration = in.readBoolean();
    int size = in.readInt();
    Set<String> groups = new HashSet<String>();
    if (size > 0) {
      for (int i=0; i<size; i++) {
        groups.add(in.readUTF());
      }
    }
    this.groups = groups;
    this.numAttempts = in.readInt();
  }
  
  public Set<String> getGroups() {
    return this.groups;
  }
  
  public void setGroups(Set<String> groups) {
    this.groups = groups;
  }
  
  public boolean isRequestForEntireConfiguration() {
    return this.isRequestForEntireConfiguration;
  }
  
  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("ConfigurationRequest for groups : ");
    sb.append("\n cluster");
    sb.append(this.groups);
    return sb.toString();
  }

  // TODO Sourabh, please review for correctness
  //Asif: Returning null, as otherwise backward compatibility tests fail
  // due to missing pre to & from data functions.
  public Version[] getSerializationVersions() {
    return null;//new Version[] { Version.CURRENT };
  }

  public int getNumAttempts() {
    return numAttempts;
  }

  public void setNumAttempts(int numAttempts) {
    this.numAttempts = numAttempts;
  }

}
