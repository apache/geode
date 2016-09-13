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
package com.gemstone.gemfire.distributed;

/**
 * Members of the distributed system can fill one or more user defined roles.
 * A role is metadata that describes how the member relates to other members
 * or what purpose it fills. 
 *
 * <a href="DistributedSystem.html#roles">Roles are specified</a> when 
 * connecting to the {@link DistributedSystem}.
 *
 * @deprecated this feature is scheduled to be removed
 */
public interface Role extends Comparable<Role> {
  
  /** 
   * Returns the name of this role. 
   *
   * @return user-defined string name of this role
   */
  public String getName();
  
  /** 
   * Returns true if this role is currently present in distributed system.
   * If true, then at least one member in the system is configured with this
   * role, regardless of whether or not that member has a cache.
   *
   * @return true if this role is currently present in distributed system
   */
  public boolean isPresent();
  
  /** 
   * Returns the count of members currently filling this role. These members
   * may or may not have a cache.
   *
   * @return number of members in system currently filling this role
   */
  public int getCount();
}

