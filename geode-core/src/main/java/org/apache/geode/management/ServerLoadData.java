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
package org.apache.geode.management;

import java.beans.ConstructorProperties;

/**
 * Composite data type used to distribute server load information.
 *
 * @since GemFire 7.0
 */
public class ServerLoadData {

  private final float connectionLoad;
  private final float subscriberLoad;
  private final float loadPerConnection;
  private final float loadPerSubscriber;

  /**
   * This constructor is to be used by internal JMX framework only. User should not try to create an
   * instance of this class.
   */
  @ConstructorProperties({"connectionLoad", "subscriberLoad", "loadPerConnection",
      "loadPerSubscriber"})
  public ServerLoadData(float connectionLoad, float subscriberLoad, float loadPerConnection,
      float loadPerSubscriber) {
    this.connectionLoad = connectionLoad;
    this.subscriberLoad = subscriberLoad;
    this.loadPerConnection = loadPerConnection;
    this.loadPerSubscriber = loadPerSubscriber;

  }

  /**
   * Returns the load on the server due to client to server connections.
   */
  public float getConnectionLoad() {
    return connectionLoad;
  }

  /**
   * Returns the load on the server due to subscription connections.
   */
  public float getSubscriberLoad() {
    return subscriberLoad;
  }

  /**
   * Returns an estimate of how much load each new connection will add to this server. The Locator
   * use this information to estimate the load on the server before it receives a new load snapshot.
   */
  public float getLoadPerConnection() {
    return loadPerConnection;
  }

  /**
   * Returns an estimate of the much load each new subscriber will add to this server. The Locator
   * uses this information to estimate the load on the server before it receives a new load
   * snapshot.
   */
  public float getLoadPerSubscriber() {
    return loadPerSubscriber;
  }

  /**
   * String representation of ServerLoadData
   */
  @Override
  public String toString() {

    return "{ServerLoad is : connectionLoad = " + connectionLoad + " subscriberLoad = "
        + subscriberLoad + " loadPerConnection = " + loadPerConnection + " loadPerSubscriber = "
        + loadPerSubscriber + " }";
  }

}
