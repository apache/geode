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
package org.apache.geode.distributed;

import java.util.List;
import java.util.Set;

/**
 * This is the fundamental representation of a member in a GemFire distributed system. A process
 * becomes a member by calling {@link DistributedSystem#connect}.
 *
 * @since GemFire 5.0
 */
public interface DistributedMember extends Comparable<DistributedMember> {

  /**
   * Returns this member's name. The member name is set using the "name" gemfire property. Returns
   * "" if the member does not have a name.
   *
   * @since GemFire 7.0
   */
  String getName();

  /**
   * Returns the canonical name of the host machine for this member.
   */
  String getHost();

  /**
   * Returns the Roles that this member performs in the system. Note that the result will contain
   * both groups and roles.
   *
   * @deprecated Roles is scheduled to be removed
   */
  @Deprecated
  Set<Role> getRoles();

  /**
   * Returns the groups this member belongs to. A member defines the groups it is in using the
   * "groups" gemfire property. Note that the deprecated "roles" gemfire property are also treated
   * as groups so this result will contain both groups and roles.
   *
   * @return a list of groups that this member belongs to.
   */
  List<String> getGroups();

  /**
   * Returns the process id for this member. This may return zero if the platform or configuration
   * does not allow native access to process info.
   */
  int getProcessId();

  /**
   * Returns a unique identifier for this member. Note that this value may change during the life
   * of the member.
   */
  String getId();

  /**
   * Returns an immutable unique identifier for this member.
   */
  String getUniqueId();

  /**
   * Returns the durable attributes for this client.
   */
  DurableClientAttributes getDurableClientAttributes();

}
