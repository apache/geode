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
 * An event that describes an operation on a cache.
 * Instances of this are delivered to a {@link SystemMemberCacheListener} when a
 * a cache is created or closed.
 *
 * @since GemFire 5.0
 */
public class SystemMemberCacheEventImpl
  extends SystemMembershipEventImpl
  implements SystemMemberCacheEvent
{

  /** The operation done by this event */
  private Operation op;

  ///////////////////////  Constructors  ///////////////////////

  /**
   * Creates a new <code>SystemMemberCacheEvent</code> for the member
   * with the given id.
   */
  protected SystemMemberCacheEventImpl(DistributedMember id, Operation op) {
    super(id);
    this.op = op;
  }

  /////////////////////  Instance Methods  /////////////////////

  public Operation getOperation() {
    return this.op;
  }

  @Override
  public String toString() {
    return super.toString() + " op=" + this.op;
  }

}
