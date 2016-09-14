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
package org.apache.geode.internal.cache;

import org.apache.geode.distributed.Role;

import java.util.Set;

/**
 * A reliable message queue is used by a DistributedRegion to queue up distributed
 * operations for required roles that are not present at the time the operation
 * is done.
 * Instances of this interface can be obtained from {@link ReliableMessageQueueFactory} which can be obtained from {@link GemFireCacheImpl#getReliableMessageQueueFactory}.
 * 
 * @since GemFire 5.0
 */
public interface ReliableMessageQueue {
  /**
   * Returns the region this queue belongs to.
   */
  public DistributedRegion getRegion();
  /**
   * Adds a message to the queue to be sent to the list of roles.
   * @param data the actual data that describes the operation to enqueue
   * @param roles the roles that need to receive this message.
   */
  public void add(ReliableDistributionData data, Set roles);
  /**
   * Gets the roles that this queue currently has messages for.
   * @return a set of {link Role}s that currently have queued messages.
   * <code>null</code> is returned if no messages are queued. 
   */
  public Set getQueuingRoles();
  /**
   * Attempts to send any messages that have been added for the given role
   * to all members that are currently playing that role.
   * @param role the role whose queued messages should be sent
   * @return true if send was successful; false if it was not and the messages
   * are still queued.
   */
  public boolean roleReady(Role role);
  /**
   * Removes all the data in this queue causing it to never be sent.
   */
  public void destroy();
  /**
   * Closes this queue. This frees up any memory used by the queue but its
   * persistent data remains.
   */
  public void close();
}
