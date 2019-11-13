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

import org.apache.geode.annotations.Experimental;

@Experimental
public class PdxInfo extends RuntimeInfo {
  private boolean readSerialized;
  private String diskStore;
  private boolean ignoreUnreadFields;
  private boolean persistent;
  private String serializerClassName;

  public boolean isReadSerialized() {
    return readSerialized;
  }

  public void setReadSerialized(boolean readSerialized) {
    this.readSerialized = readSerialized;
  }

  public String getDiskStore() {
    return diskStore;
  }

  public void setDiskStore(String diskStore) {
    this.diskStore = diskStore;
  }

  public boolean isIgnoreUnreadFields() {
    return ignoreUnreadFields;
  }

  public void setIgnoreUnreadFields(boolean ignoreUnreadFields) {
    this.ignoreUnreadFields = ignoreUnreadFields;
  }

  public boolean isPersistent() {
    return persistent;
  }

  public void setPersistent(boolean persistent) {
    this.persistent = persistent;
  }

  public String getSerializerClassName() {
    return serializerClassName;
  }

  public void setSerializerClassName(String serializerClassName) {
    this.serializerClassName = serializerClassName;
  }
}
