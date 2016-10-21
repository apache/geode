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

package org.apache.geode.modules.session.internal.filter;

import java.io.Serializable;
import javax.servlet.http.HttpSessionBindingEvent;
import javax.servlet.http.HttpSessionBindingListener;

public class HttpSessionBindingListenerImpl extends AbstractListener
    implements HttpSessionBindingListener, Serializable {

  public HttpSessionBindingListenerImpl(int i) {
    super(i);
  }

  @Override
  public synchronized void valueBound(HttpSessionBindingEvent event) {
    events.add(ListenerEventType.SESSION_VALUE_BOUND);
    latch.countDown();
  }

  @Override
  public synchronized void valueUnbound(HttpSessionBindingEvent event) {
    events.add(ListenerEventType.SESSION_VALUE_UNBOUND);
    latch.countDown();
  }
}
