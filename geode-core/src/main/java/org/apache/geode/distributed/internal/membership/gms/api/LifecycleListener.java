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
package org.apache.geode.distributed.internal.membership.gms.api;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;

public interface LifecycleListener {

  void start(
      final MemberIdentifier memberID);

  boolean disconnect(Exception exception);

  /**
   * TODO - (GEODE-7464) it seems like this could really just happen during
   * {@link #start(MemberIdentifier)}
   * The member ID passed to this method is exactly the same id as the one passed to start. However,
   * this method has side affects in DirectChannel which may or may not need to happen at the point
   * in time this method is called?
   */
  void setLocalAddress(InternalDistributedMember address);

  void destroyMember(InternalDistributedMember member, String reason);
}
