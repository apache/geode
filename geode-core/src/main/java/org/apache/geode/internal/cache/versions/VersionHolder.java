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
package org.apache.geode.internal.cache.versions;

/**
 * Read only interface for an object that holds an entry version.
 *
 * @param <T>
 */
public interface VersionHolder<T extends VersionSource> {
  /**
   * @return the current version number for the corresponding entry
   */
  int getEntryVersion();
  
  /**
   * @return the region version number for the last modification
   */
  long getRegionVersion();
  
  /**
   * @return the time stamp of the operation
   */
  long getVersionTimeStamp();
  
  /**
   * @return the ID of the member that last changed the corresponding entry
   */
  T getMemberID();
  
  /**
   * @return the Distributed System Id of the system that last changed the corresponding entry
   */
  int getDistributedSystemId();
  
  /** get rvv internal high byte.  Used by region entries for transferring to storage */
  public short getRegionVersionHighBytes();
  
  /** get rvv internal low bytes.  Used by region entries for transferring to storage */
  public int getRegionVersionLowBytes();

}
