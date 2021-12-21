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
package org.apache.geode.cache.server;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializable;


/**
 * A data object containing the load information for a cache server. This object is returned from
 * {@link ServerLoadProbe#getLoad(ServerMetrics)} and indicates how heavily loaded the server is.
 *
 * The values returned by {@link #getConnectionLoad()} and {@link #getSubscriptionConnectionLoad()}
 * can be any number greater than 0. A larger load value means that the server has more load.
 *
 * The values returned by getLoadPerConnection() and getLoadPerSubscriptionConnection are used to
 * estimate the effect of new connections before the connects are actually made to this server. The
 * load is estimated as <code>
 * load + loadPerConnection*numAdditionalConnections.
 * </code>
 *
 * @since GemFire 5.7
 *
 */
public final class ServerLoad implements DataSerializable {
  private static final long serialVersionUID = -4498005291184650907L;
  private float connectionLoad;
  private float subscriberLoad;
  private float loadPerConnection;
  private float loadPerSubscriber;


  public ServerLoad(float connectionLoad, float loadPerConnection, float subscriptionConnectionLoad,
      float loadPerSubscriptionConnection) {
    super();
    this.connectionLoad = connectionLoad;
    subscriberLoad = subscriptionConnectionLoad;
    this.loadPerConnection = loadPerConnection;
    loadPerSubscriber = loadPerSubscriptionConnection;
  }

  public ServerLoad() {

  }

  /**
   * Get the load on the server due to client to server connections.
   *
   * @return a float greater than or equals to 0
   */
  public float getConnectionLoad() {
    return connectionLoad;
  }

  /**
   * Get an estimate of the how much load each new connection will add to this server. The locator
   * uses this information to estimate the load on the server before it receives a new load
   * snapshot.
   *
   * @return a float greater than or equals to 0
   */
  public float getLoadPerConnection() {
    return loadPerConnection;
  }

  /**
   * Get the load on the server due to subscription connections.
   *
   * @return a float greater than or equals to 0
   */
  public float getSubscriptionConnectionLoad() {
    return subscriberLoad;
  }

  /**
   * Get an estimate of the how much load each new subscriber will add to this server. The locator
   * uses this information to estimate the load on the server before it receives a new load
   * snapshot.
   *
   * @return a float greater than or equals to 0
   */
  public float getLoadPerSubscriptionConnection() {
    return loadPerSubscriber;
  }

  /**
   * Set the load due to client to server connections.
   */
  public void setConnectionLoad(float connectionLoad) {
    this.connectionLoad = connectionLoad;
  }

  /**
   * Set the load due to client subscriptions.
   */
  public void setSubscriptionConnectionLoad(float subscriberLoad) {
    this.subscriberLoad = subscriberLoad;
  }

  /**
   * Set the estimated load per connection.
   */
  public void setLoadPerConnection(float loadPerConnection) {
    this.loadPerConnection = loadPerConnection;
  }

  /**
   * Set the estimated load per subscription connection.
   */
  public void setLoadPerSubscriptionConnection(float loadPerSubscriber) {
    this.loadPerSubscriber = loadPerSubscriber;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    out.writeFloat(connectionLoad);
    out.writeFloat(loadPerConnection);
    out.writeFloat(subscriberLoad);
    out.writeFloat(loadPerSubscriber);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    connectionLoad = in.readFloat();
    loadPerConnection = in.readFloat();
    subscriberLoad = in.readFloat();
    loadPerSubscriber = in.readFloat();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Float.floatToIntBits(connectionLoad);
    result = prime * result + Float.floatToIntBits(loadPerConnection);
    result = prime * result + Float.floatToIntBits(loadPerSubscriber);
    result = prime * result + Float.floatToIntBits(subscriberLoad);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final ServerLoad other = (ServerLoad) obj;
    if (Float.floatToIntBits(connectionLoad) != Float.floatToIntBits(other.connectionLoad)) {
      return false;
    }
    if (Float.floatToIntBits(loadPerConnection) != Float.floatToIntBits(other.loadPerConnection)) {
      return false;
    }
    if (Float.floatToIntBits(loadPerSubscriber) != Float.floatToIntBits(other.loadPerSubscriber)) {
      return false;
    }
    return Float.floatToIntBits(subscriberLoad) == Float.floatToIntBits(other.subscriberLoad);
  }

  @Override
  public String toString() {
    return "Load(" + connectionLoad + ", " + loadPerConnection + ", " + subscriberLoad + ", "
        + loadPerSubscriber + ")";
  }
}
