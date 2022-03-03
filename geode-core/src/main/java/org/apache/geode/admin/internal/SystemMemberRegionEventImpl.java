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
package org.apache.geode.admin.internal;

import org.apache.geode.admin.SystemMemberCacheListener;
import org.apache.geode.admin.SystemMemberRegionEvent;
import org.apache.geode.cache.Operation;
import org.apache.geode.distributed.DistributedMember;

/**
 * An event that describes an operation on a region. Instances of this are delivered to a
 * {@link SystemMemberCacheListener} when a a region comes or goes.
 *
 * @since GemFire 5.0
 */
public class SystemMemberRegionEventImpl extends SystemMemberCacheEventImpl
    implements SystemMemberRegionEvent {

  /**
   * The path of region created/destroyed
   */
  private final String regionPath;

  /////////////////////// Constructors ///////////////////////

  /**
   * Creates a new <code>SystemMemberRegionEvent</code> for the member with the given id.
   */
  protected SystemMemberRegionEventImpl(DistributedMember id, Operation op, String regionPath) {
    super(id, op);
    this.regionPath = regionPath;
  }

  ///////////////////// Instance Methods /////////////////////

  @Override
  public String getRegionPath() {
    return regionPath;
  }

  @Override
  public String toString() {
    return super.toString() + " region=" + regionPath;
  }

}
