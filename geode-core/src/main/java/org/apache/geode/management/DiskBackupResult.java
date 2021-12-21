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
 * Composite data type used to distribute the results of a disk backup operation.
 *
 * @since GemFire 7.0
 */
public class DiskBackupResult {

  /**
   * Returns the name of the directory
   */
  private final String diskDirectory;

  /**
   * whether the bacup operation was successful or not
   */
  private final boolean offilne;

  /**
   * This constructor is to be used by internal JMX framework only. User should not try to create an
   * instance of this class.
   */
  @ConstructorProperties({"diskDirectory", "offilne"

  })
  public DiskBackupResult(String diskDirectory, boolean offline) {
    this.diskDirectory = diskDirectory;
    offilne = offline;
  }

  /**
   * Returns the name of the directory where the files for this backup were written.
   */
  public String getDiskDirectory() {
    return diskDirectory;
  }

  /**
   * Returns whether the backup was successful.
   *
   * @return True if the backup was successful, false otherwise.
   */
  public boolean isOffilne() {
    return offilne;
  }
}
