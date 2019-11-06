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

package org.apache.geode.management.configuration;



import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.management.runtime.RuntimeInfo;

/**
 * Configuration Objects for Pdx in the cache
 */
@Experimental
public class Pdx extends AbstractConfiguration<RuntimeInfo> {
  public static final String PDX_ID = "PDX";
  public static final String PDX_ENDPOINT = "/configurations/pdx";

  private Boolean readSerialized;
  private ClassName pdxSerializer;
  private AutoSerializer autoSerializer;
  private Boolean ignoreUnreadFields;
  private String diskStoreName;

  /**
   * Returns {@link #PDX_ID}
   */
  @Override
  @JsonIgnore
  public String getId() {
    return PDX_ID;
  }

  @Override
  public Links getLinks() {
    Links links = new Links(getId(), PDX_ENDPOINT);
    links.setSelf(PDX_ENDPOINT);
    return links;
  }

  public AutoSerializer getAutoSerializer() {
    return autoSerializer;
  }

  public void setAutoSerializer(AutoSerializer autoSerializer) {
    this.autoSerializer = autoSerializer;
  }

  public Boolean isReadSerialized() {
    return readSerialized;
  }

  public void setReadSerialized(Boolean readSerialized) {
    this.readSerialized = readSerialized;
  }

  public ClassName getPdxSerializer() {
    return pdxSerializer;
  }

  public void setPdxSerializer(ClassName pdxSerializer) {
    this.pdxSerializer = pdxSerializer;
  }

  public Boolean isIgnoreUnreadFields() {
    return ignoreUnreadFields;
  }

  public void setIgnoreUnreadFields(Boolean ignoreUnreadFields) {
    this.ignoreUnreadFields = ignoreUnreadFields;
  }

  public String getDiskStoreName() {
    return diskStoreName;
  }

  public void setDiskStoreName(String diskStoreName) {
    this.diskStoreName = diskStoreName;
  }
}
