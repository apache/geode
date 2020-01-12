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

package org.apache.geode.management.runtime;

import java.util.Objects;

import org.apache.geode.annotations.Experimental;

@Experimental
public class PdxInfo extends RuntimeInfo {
  private boolean readSerialized;
  private String diskStoreName;
  private boolean ignoreUnreadFields;
  private String pdxSerializer;

  public boolean isReadSerialized() {
    return readSerialized;
  }

  public void setReadSerialized(boolean readSerialized) {
    this.readSerialized = readSerialized;
  }

  public String getDiskStoreName() {
    return diskStoreName;
  }

  public void setDiskStoreName(String diskStoreName) {
    this.diskStoreName = diskStoreName;
  }

  public boolean isIgnoreUnreadFields() {
    return ignoreUnreadFields;
  }

  public void setIgnoreUnreadFields(boolean ignoreUnreadFields) {
    this.ignoreUnreadFields = ignoreUnreadFields;
  }

  public String getPdxSerializer() {
    return pdxSerializer;
  }

  public void setPdxSerializer(String pdxSerializer) {
    this.pdxSerializer = pdxSerializer;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PdxInfo)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    PdxInfo pdxInfo = (PdxInfo) o;
    return isReadSerialized() == pdxInfo.isReadSerialized() &&
        isIgnoreUnreadFields() == pdxInfo.isIgnoreUnreadFields() &&
        Objects.equals(getDiskStoreName(), pdxInfo.getDiskStoreName()) &&
        Objects.equals(getPdxSerializer(), pdxInfo.getPdxSerializer());
  }

  @Override
  public int hashCode() {
    return Objects
        .hash(super.hashCode(), isReadSerialized(), getDiskStoreName(), isIgnoreUnreadFields(),
            getPdxSerializer());
  }
}
