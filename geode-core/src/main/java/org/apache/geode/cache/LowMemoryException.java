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

package org.apache.geode.cache;

import java.util.Collections;
import java.util.Set;

import org.apache.geode.cache.control.ResourceManager;
import org.apache.geode.distributed.DistributedMember;

/**
 * Indicates a low memory condition either on the local or a remote {@link Cache}. The
 * {@link ResourceManager} monitors local tenured memory consumption and determines when operations
 * are rejected.
 *
 * @see ResourceManager#setCriticalHeapPercentage(float)
 * @see Region#put(Object, Object)
 *
 * @since GemFire 6.0
 */
public class LowMemoryException extends ResourceException {

  private static final long serialVersionUID = 6585765466722883168L;
  private final Set<DistributedMember> critMems;

  /**
   * Creates a new instance of <code>LowMemoryException</code>.
   */
  public LowMemoryException() {
    critMems = Collections.emptySet();
  }

  /**
   * Constructs an instance of <code>LowMemoryException</code> with the specified detail message.
   *
   * @param msg the detail message
   * @param criticalMembers the member(s) which are/were in a critical state
   */
  public LowMemoryException(String msg, final Set<DistributedMember> criticalMembers) {
    super(msg);
    critMems = Collections.unmodifiableSet(criticalMembers);
  }

  /**
   * Get a read-only set of members in a critical state at the time this exception was constructed.
   *
   * @return the critical members
   */
  public Set<DistributedMember> getCriticalMembers() {
    return critMems;
  }
}
