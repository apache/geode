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
package com.gemstone.gemfire.modules.session.catalina.callback;

import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.modules.session.catalina.DeltaSession;
import com.gemstone.gemfire.modules.session.catalina.DeltaSessionManager;
import com.gemstone.gemfire.modules.util.ContextMapper;

import javax.servlet.http.HttpSession;
import java.util.Properties;

public class SessionExpirationCacheListener extends CacheListenerAdapter<String, HttpSession> implements Declarable {

  public void afterDestroy(EntryEvent<String, HttpSession> event) {
    // A Session expired. If it was destroyed by GemFire expiration, process it.
    // If it was destroyed via Session.invalidate, ignore it since it has
    // already been processed.
    DeltaSession session = null;
    if (event.getOperation() == Operation.EXPIRE_DESTROY) {
      session = (DeltaSession) event.getOldValue();
    } else {
      /*
       * This comes into play when we're dealing with an empty client proxy. We
       * need the actual destroyed object to come back from the server so that
       * any associated listeners can fire correctly. Having the destroyed
       * object come back as the callback arg depends on setting the property
       * gemfire.EXPIRE_SENDS_ENTRY_AS_CALLBACK.
       */
      Object callback = event.getCallbackArgument();
      if (callback != null && callback instanceof DeltaSession) {
        session = (DeltaSession) callback;
        DeltaSessionManager m = ContextMapper.getContext(session.getContextName());
        if (m != null) {
          session.setOwner(m);
        }
      }
    }
    if (session != null) {
      session.processExpired();
    }
  }

  public void init(Properties p) {
  }

  public boolean equals(Object obj) {
    // This method is only implemented so that RegionAttributesCreation.sameAs
    // works properly.
    if (this == obj) {
      return true;
    }

    if (obj == null || !(obj instanceof SessionExpirationCacheListener)) {
      return false;
    }

    return true;
  }
}
