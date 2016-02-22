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

package com.gemstone.gemfire.modules.session.internal.common;

import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;

import java.util.Properties;
import javax.servlet.http.HttpSession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SessionExpirationCacheListener extends
    CacheListenerAdapter<String, HttpSession> implements Declarable {

  private static final Logger LOG =
      LoggerFactory.getLogger(SessionExpirationCacheListener.class.getName());

  @Override
  public void afterDestroy(EntryEvent<String, HttpSession> event) {
    /**
     * A Session expired. If it was destroyed by GemFire expiration,
     * process it. If it was destroyed via Session.invalidate, ignore it
     * since it has already been processed.
     */
    if (event.getOperation() == Operation.EXPIRE_DESTROY) {
      HttpSession session = (HttpSession) event.getOldValue();
      session.invalidate();
    }
  }

  @Override
  public void init(Properties p) {
  }
}
