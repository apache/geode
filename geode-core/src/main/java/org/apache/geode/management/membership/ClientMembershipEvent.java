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
package com.gemstone.gemfire.management.membership;

import com.gemstone.gemfire.distributed.DistributedMember;

/**
 * An event delivered to a {@link ClientMembershipListener} when this process
 * detects connection changes to ClientServers or clients.
 *
 * @since GemFire 8.0
 */
public interface ClientMembershipEvent {

  /**
   * Returns the the member that connected or disconnected.
   *
   * @see com.gemstone.gemfire.distributed.DistributedSystem#getDistributedMember
   */
  public DistributedMember getMember();

  /**
   * Returns the id of the member that connected or disconnected.
   *
   * @see com.gemstone.gemfire.distributed.DistributedMember#getId
   */
  public String getMemberId();

  /**
   * Returns true if the member is a client to a CacheServer hosted by
   * this process. Returns false if the member is a peer that this
   * process is connected to.
   */
  public boolean isClient();

}
