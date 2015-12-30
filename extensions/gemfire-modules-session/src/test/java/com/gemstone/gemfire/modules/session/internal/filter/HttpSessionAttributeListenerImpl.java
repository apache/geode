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

import javax.servlet.http.HttpSessionAttributeListener;
import javax.servlet.http.HttpSessionBindingEvent;

/**
 *
 */
public class HttpSessionAttributeListenerImpl extends AbstractListener
    implements HttpSessionAttributeListener {

  @Override
  public synchronized void attributeAdded(HttpSessionBindingEvent se) {
    events.add(ListenerEventType.SESSION_ATTRIBUTE_ADDED);
    latch.countDown();
  }

  @Override
  public synchronized void attributeRemoved(HttpSessionBindingEvent se) {
    events.add(ListenerEventType.SESSION_ATTRIBUTE_REMOVED);
    latch.countDown();
  }

  @Override
  public synchronized void attributeReplaced(HttpSessionBindingEvent se) {
    events.add(ListenerEventType.SESSION_ATTRIBUTE_REPLACED);
    latch.countDown();
  }
}
