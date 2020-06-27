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
package org.apache.geode.modules.session.catalina.callback;

import javax.servlet.http.HttpSession;

import org.apache.catalina.session.ManagerBase;

import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.modules.session.catalina.DeltaSessionInterface;
import org.apache.geode.modules.util.ContextMapper;

public class SessionExpirationCacheListener extends CacheListenerAdapter<String, HttpSession>
    implements Declarable {

  @Override
  public void afterDestroy(EntryEvent<String, HttpSession> event) {
    // A Session expired. If it was destroyed by Geode expiration, process it.
    // If it was destroyed via Session.invalidate, ignore it since it has
    // already been processed.
    DeltaSessionInterface session = null;
    if (event.getOperation() == Operation.EXPIRE_DESTROY) {
      session = (DeltaSessionInterface) event.getOldValue();
    } else {
      /*
       * This comes into play when we're dealing with an empty client proxy. We need the actual
       * destroyed object to come back from the server so that any associated listeners can fire
       * correctly. Having the destroyed object come back as the callback arg depends on setting the
       * property gemfire.EXPIRE_SENDS_ENTRY_AS_CALLBACK.
       */
      Object callback = event.getCallbackArgument();
      if (callback instanceof DeltaSessionInterface) {
        session = (DeltaSessionInterface) callback;
        ManagerBase m = ContextMapper.getContext(session.getContextName());
        if (m != null) {
          session.setOwner(m);
        }
      }
    }

    if (session != null) {
      session.processExpired();
    }
  }

  @Override
  public boolean equals(Object obj) {
    // Only implemented so that RegionAttributesCreation.sameAs works properly.
    if (this == obj) {
      return true;
    }

    return (obj instanceof SessionExpirationCacheListener);
  }

  @Override
  public int hashCode() {
    return SessionExpirationCacheListener.class.hashCode();
  }
}
