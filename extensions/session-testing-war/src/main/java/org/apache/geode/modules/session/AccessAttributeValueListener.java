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
package org.apache.geode.modules.session;

import javax.servlet.http.HttpSessionAttributeListener;
import javax.servlet.http.HttpSessionBindingEvent;

public class AccessAttributeValueListener implements HttpSessionAttributeListener {
  @Override
  public void attributeAdded(HttpSessionBindingEvent event) {
    System.out.println("event created value is " + event.getValue());
  }

  @Override
  public void attributeRemoved(HttpSessionBindingEvent event) {
    System.out.println("event removed value is " + event.getValue());
  }

  @Override
  public void attributeReplaced(HttpSessionBindingEvent event) {
    System.out.println("event replaced value is " + event.getValue());
  }
}
