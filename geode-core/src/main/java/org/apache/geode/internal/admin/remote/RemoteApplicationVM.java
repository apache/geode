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


package org.apache.geode.internal.admin.remote;

import java.util.Objects;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.admin.Alert;
import org.apache.geode.internal.admin.ApplicationVM;

/**
 * Represents an application VM (member of the distributed system).
 */
public class RemoteApplicationVM extends RemoteGemFireVM implements ApplicationVM {

  // constructors

  /**
   * Creates a <code>RemoteApplicationVM</code> in a given distributed system (<code>agent</code>)
   * with the given <code>id</code>.
   * <p/>
   * You MUST invoke {@link RemoteGemFireVM#startStatDispatcher()} immediately after constructing an
   * instance.
   *
   * @param alertLevel The level of {@link Alert}s that this administration console should receive
   *        from this member of the distributed system.
   */
  public RemoteApplicationVM(RemoteGfManagerAgent agent, InternalDistributedMember id,
      int alertLevel) {
    super(agent, id, alertLevel);
  }

  // Object methods

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof RemoteApplicationVM)) {
      return false;
    } else {
      RemoteApplicationVM vm = (RemoteApplicationVM) obj;
      return (agent == vm.agent) && id.equals(vm.id);
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(agent, id);
  }
}
