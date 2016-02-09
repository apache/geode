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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.gemstone.gemfire.management.internal.cli.json.GfJsonObject;
import com.gemstone.gemfire.management.internal.cli.result.CliJsonSerializable;
import com.gemstone.gemfire.management.internal.cli.util.JsonUtil;


public class RegionAttributesInfo implements CliJsonSerializable {
  private static Map<String, String> nameToDisplayName = new HashMap<String, String>();
  
  static {
    nameToDisplayName.put("cacheLoader",     "Cache Loader");
    nameToDisplayName.put("cacheWriter",     "Cache Writer");
    nameToDisplayName.put("keyConstraint",   "Key Constraint");
    nameToDisplayName.put("valueConstraint", "Value Constraint");
  }
  
  private String cacheLoader;
  private String cacheWriter;
  private String keyConstraint;
  private String valueConstraint;
  private String[] fieldsToSkipOnUI;
  
  public RegionAttributesInfo() {}

  /**
   * @param cacheLoader
   * @param cacheWriter
   * @param keyConstraint
   * @param valueConstraint
   */
  public RegionAttributesInfo(String cacheLoader, String cacheWriter,
      String keyConstraint, String valueConstraint) {
    this.cacheLoader = cacheLoader;
    this.cacheWriter = cacheWriter;
    this.keyConstraint = keyConstraint;
    this.valueConstraint = valueConstraint;
  }
  
  public String getCacheLoader() {
    return cacheLoader;
  }
  public String getCacheWriter() {
    return cacheWriter;
  }
  public String getKeyConstraint() {
    return keyConstraint;
  }
  public String getValueConstraint() {
    return valueConstraint;
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
  public int getJSId() {
    return CLI_DOMAIN_OBJECT__REGION_ATTR_INFO;
  }
  
  @Override
  public Map<String, String> getFieldNameToDisplayName() {
    return nameToDisplayName;
  }
  @Override
  public void fromJson(GfJsonObject objectStateAsjson) {
    this.cacheLoader = JsonUtil.getString(objectStateAsjson, "cacheLoader");
    this.cacheWriter = JsonUtil.getString(objectStateAsjson, "cacheWriter");
    this.keyConstraint = JsonUtil.getString(objectStateAsjson, "keyConstraint");
    this.valueConstraint = JsonUtil.getString(objectStateAsjson, "valueConstraint");
  }

  @Override
  public int hashCode() { // eclipse generated
    final int prime = 31;
    int result = 1;
    result = prime * result
        + ((cacheLoader == null) ? 0 : cacheLoader.hashCode());
    result = prime * result
        + ((cacheWriter == null) ? 0 : cacheWriter.hashCode());
    result = prime * result + Arrays.hashCode(fieldsToSkipOnUI);
    result = prime * result
        + ((keyConstraint == null) ? 0 : keyConstraint.hashCode());
    result = prime * result
        + ((valueConstraint == null) ? 0 : valueConstraint.hashCode());
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
    RegionAttributesInfo other = (RegionAttributesInfo) obj;
    if (cacheLoader == null) {
      if (other.cacheLoader != null)
        return false;
    } else if (!cacheLoader.equals(other.cacheLoader))
      return false;
    if (cacheWriter == null) {
      if (other.cacheWriter != null)
        return false;
    } else if (!cacheWriter.equals(other.cacheWriter))
      return false;
    if (!Arrays.equals(fieldsToSkipOnUI, other.fieldsToSkipOnUI))
      return false;
    if (keyConstraint == null) {
      if (other.keyConstraint != null)
        return false;
    } else if (!keyConstraint.equals(other.keyConstraint))
      return false;
    if (valueConstraint == null) {
      if (other.valueConstraint != null)
        return false;
    } else if (!valueConstraint.equals(other.valueConstraint))
      return false;
    return true;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("RegionAttributesInfo [cacheLoader=");
    builder.append(cacheLoader);
    builder.append(", cacheWriter=");
    builder.append(cacheWriter);
    builder.append(", keyConstraint=");
    builder.append(keyConstraint);
    builder.append(", valueConstraint=");
    builder.append(valueConstraint);
    builder.append("]");
    return builder.toString();
  }
}
