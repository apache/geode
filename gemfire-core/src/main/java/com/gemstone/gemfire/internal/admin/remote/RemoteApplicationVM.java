/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
   
   
package com.gemstone.gemfire.internal.admin.remote;

import com.gemstone.gemfire.internal.admin.*;
import com.gemstone.gemfire.distributed.internal.membership.*;

/**
 * Represents an application VM (member of the distributed system).
 */
public final class RemoteApplicationVM extends RemoteGemFireVM
  implements ApplicationVM {

  // constructors

  /**
   * Creates a <code>RemoteApplicationVM</code> in a given distributed
   * system (<code>agent</code>) with the given <code>id</code>.
   * <p/>
   * You MUST invoke {@link RemoteGemFireVM#startStatDispatcher()} immediately after
   * constructing an instance.
   * 
   * @param alertLevel
   *        The level of {@link Alert}s that this administration
   *        console should receive from this member of the distributed
   *        system. 
   */
  public RemoteApplicationVM(RemoteGfManagerAgent agent,
                             InternalDistributedMember id, int alertLevel) {
    super(agent, id, alertLevel);
  }

  // Object methods

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof RemoteApplicationVM)) {
      return false;
    } else {
      RemoteApplicationVM vm = (RemoteApplicationVM)obj;
      return (this.agent == vm.agent) && this.id.equals(vm.id);
    }
  }

}
