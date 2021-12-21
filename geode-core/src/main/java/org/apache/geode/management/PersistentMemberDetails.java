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
package org.apache.geode.management;

import java.beans.ConstructorProperties;

/**
 * Composite data type used to distribute attributes for the missing disk store of a persistent
 * member.
 *
 * @since GemFire 7.0
 *
 */
public class PersistentMemberDetails {

  private final String host;
  private final String directory;
  private final String diskStoreId;

  /**
   *
   * This constructor is to be used by internal JMX framework only. User should not try to create an
   * instance of this class.
   */
  @ConstructorProperties({"host", "directory", "diskStoreId"})
  public PersistentMemberDetails(final String host, final String directory,
      final String diskStoreId) {
    this.host = host;
    this.directory = directory;
    this.diskStoreId = diskStoreId;
  }

  /**
   * Returns the name or IP address of the host on which the member is running.
   */
  public String getHost() {
    return host;
  }

  /**
   * Returns the directory in which the <code>DiskStore</code> is saved.
   */
  public String getDirectory() {
    return directory;
  }

  /**
   * Returns the ID of the <code>DiskStore</code>.
   */
  public String getDiskStoreId() {
    return diskStoreId;
  }
}
