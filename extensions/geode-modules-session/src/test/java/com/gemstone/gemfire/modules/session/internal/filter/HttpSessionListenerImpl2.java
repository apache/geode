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

package com.gemstone.gemfire.modules.session.internal.filter;

import com.gemstone.gemfire.modules.session.filter.SessionCachingFilter;

import javax.servlet.http.HttpSession;
import javax.servlet.http.HttpSessionEvent;
import javax.servlet.http.HttpSessionListener;

public class HttpSessionListenerImpl2 extends AbstractListener
    implements HttpSessionListener {

  @Override
  public void sessionCreated(HttpSessionEvent se) {
    events.add(ListenerEventType.SESSION_CREATED);
    latch.countDown();
  }

  @Override
  public void sessionDestroyed(HttpSessionEvent se) {
    HttpSession gfeSession = SessionCachingFilter.getWrappingSession(
        se.getSession());
    assert (gfeSession != null);
    events.add(ListenerEventType.SESSION_DESTROYED);
    latch.countDown();
  }
}
