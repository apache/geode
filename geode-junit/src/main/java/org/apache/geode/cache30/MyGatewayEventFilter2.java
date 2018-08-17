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
package org.apache.geode.cache30;

import java.util.Properties;

import org.apache.geode.cache.util.GatewayEvent;
import org.apache.geode.cache.wan.GatewayEventFilter;
import org.apache.geode.cache.wan.GatewayQueueEvent;
import org.apache.geode.internal.cache.xmlcache.Declarable2;

public class MyGatewayEventFilter2 implements GatewayEventFilter, Declarable2 {


  public void close() {

  }

  public Properties getConfig() {
    // TODO Auto-generated method stub
    return null;
  }

  public void init(Properties props) {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.geode.cache.wan.GatewayEventFilter#afterAcknowledgement(org.apache.geode.cache.util.
   * GatewayEvent)
   */
  public void afterAcknowledgement(GatewayEvent event) {
    // TODO Auto-generated method stub

  }

  public boolean beforeEnqueue(GatewayQueueEvent event) {
    // TODO Auto-generated method stub
    return false;
  }

  public boolean beforeTransmit(GatewayQueueEvent event) {
    // TODO Auto-generated method stub
    return false;
  }

  public void afterAcknowledgement(GatewayQueueEvent event) {
    // TODO Auto-generated method stub

  }
}
