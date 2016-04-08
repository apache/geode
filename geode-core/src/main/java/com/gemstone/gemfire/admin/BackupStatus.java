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
package com.gemstone.gemfire.admin;

import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.cache.persistence.PersistentID;
import com.gemstone.gemfire.distributed.DistributedMember;

/**
 * The status of a backup operation, returned by
 * {@link AdminDistributedSystem#backupAllMembers(java.io.File,java.io.File)}.
 * 
 * @since 6.5 
 * @deprecated as of 7.0 use the <code><a href="{@docRoot}/com/gemstone/gemfire/management/package-summary.html">management</a></code> package instead
 */
public interface BackupStatus {
  
  /**
   * Returns a map of disk stores that were successfully backed up.
   * The key is an online distributed member. The value is the set of disk 
   * stores on that distributed member. 
   */
  Map<DistributedMember, Set<PersistentID>> getBackedUpDiskStores();
  
  /**
   * Returns the set of disk stores that were known to be offline at the 
   * time of the backup. These members were not backed up. If this set
   * is not empty the backup may not contain a complete snapshot of 
   * any partitioned regions in the distributed system.
   */
  Set<PersistentID> getOfflineDiskStores();
}
