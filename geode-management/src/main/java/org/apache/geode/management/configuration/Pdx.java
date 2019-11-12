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
 * Used to configure PDX serialization for a cache.
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

  /**
   * @throws IllegalArgumentException if a pdxSerializer has already been set
   */
  public void setAutoSerializer(AutoSerializer autoSerializer) {
    if (pdxSerializer != null) {
      throw new IllegalArgumentException(
          "The autoSerializer can not be set if a pdxSerializer is already set.");
    }
    this.autoSerializer = autoSerializer;
  }

  public Boolean isReadSerialized() {
    return readSerialized;
  }

  /**
   * Setting readSerialized to true causes any pdx deserialization to produce
   * instances of {@link org.apache.geode.pdx.PdxInstance} instead of a domain class instance.
   */
  public void setReadSerialized(Boolean readSerialized) {
    this.readSerialized = readSerialized;
  }

  public ClassName getPdxSerializer() {
    return pdxSerializer;
  }

  /**
   * @param pdxSerializer the class name given must implement the
   *        {@link org.apache.geode.pdx.PdxSerializer} interface
   * @throws IllegalArgumentException if an autoSerializer has already been set
   */
  public void setPdxSerializer(ClassName pdxSerializer) {
    if (autoSerializer != null) {
      throw new IllegalArgumentException(
          "The pdxSerializer can not be set if an autoSerializer is already set.");
    }
    this.pdxSerializer = pdxSerializer;
  }

  public Boolean isIgnoreUnreadFields() {
    return ignoreUnreadFields;
  }

  /**
   * Setting ignoreUnreadFields to true can save memory during pdx deserialization but if the
   * deserialized
   * object is reserialized then the unread field data will be lost. Unread fields will only exist
   * if a class
   * serialized with pdx has multiple versions.
   */
  public void setIgnoreUnreadFields(Boolean ignoreUnreadFields) {
    this.ignoreUnreadFields = ignoreUnreadFields;
  }

  public String getDiskStoreName() {
    return diskStoreName;
  }

  /**
   * Setting a non-null diskStoreName causes the Pdx information
   * to be persisted to the named disk store.
   * To configure Pdx to use the default disk store, set
   * the diskStoreName to "DEFAULT".
   */
  public void setDiskStoreName(String diskStoreName) {
    this.diskStoreName = diskStoreName;
  }
}
