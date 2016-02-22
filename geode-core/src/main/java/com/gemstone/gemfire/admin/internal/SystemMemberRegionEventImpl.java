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
package com.gemstone.gemfire.admin.internal;

import com.gemstone.gemfire.admin.*;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.cache.Operation;

/**
 * An event that describes an operation on a region.
 * Instances of this are delivered to a {@link SystemMemberCacheListener} when a
 * a region comes or goes.
 *
 * @since 5.0
 */
public class SystemMemberRegionEventImpl
  extends SystemMemberCacheEventImpl
  implements SystemMemberRegionEvent
{

  /** 
   * The path of region created/destroyed 
   */
  private final String regionPath;

  ///////////////////////  Constructors  ///////////////////////

  /**
   * Creates a new <code>SystemMemberRegionEvent</code> for the member
   * with the given id.
   */
  protected SystemMemberRegionEventImpl(DistributedMember id, Operation op, String regionPath) {
    super(id, op);
    this.regionPath = regionPath;
  }

  /////////////////////  Instance Methods  /////////////////////

  public String getRegionPath() {
    return this.regionPath;
  }

  @Override
  public String toString() {
    return super.toString() + " region=" + this.regionPath;
  }

}
