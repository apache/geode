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
package org.apache.geode.distributed;

import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;

/**
 * Class <code>DurableClientAttributes</code> represents durable member attributes.
 */
public class DurableClientAttributes {

  private final String poolSeparator = "_gem_";

  /**
   * The durable id of the member.
   */
  private String id;

  /**
   * The durable timeout of the client in seconds. The resources of a disconnected durable client
   * will be cleaned up if the client hasn't reconnected within this time period.
   */
  private int timeout;

  /**
   * Durable client associated with pool
   */
  private String poolName;

  public DurableClientAttributes() {}

  /**
   * Constructor.
   * 
   * @param id The id of the durable client.
   * @param timeout The timeout period of the durable client.
   */
  public DurableClientAttributes(String id, int timeout) {
    this.id = id;
    this.timeout = timeout;
    this.poolName = null;
    int pIdx = id.indexOf(poolSeparator);
    if (pIdx != -1) {
      this.poolName = id.substring(pIdx + poolSeparator.length());
      this.id = id.substring(0, pIdx);
    }

  }

  /**
   * Returns the durable client's id.
   * 
   * @return the durable client's id
   */
  public String getId() {
    if (this.id == null || this.id.isEmpty()) {
      return this.id;
    }
    String pn = ClientProxyMembershipID.getPoolName();
    if (pn != null) {
      this.poolName = pn;
    }
    String result = this.id;
    if (this.poolName != null) {
      result += poolSeparator + this.poolName;
    }
    return result;
  }

  /**
   * Returns the durable client's timeout.
   * 
   * @return the durable client's timeout
   */
  public int getTimeout() {
    return this.timeout;
  }

  /**
   * Used to update the timeout when a durable client comes back to a server
   */
  public void updateTimeout(int newValue) {
    this.timeout = newValue;
  }

  public void setPoolName(String poolName) {
    this.poolName = poolName;
  }

  public String getPoolName() {
    return this.poolName;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this)
      return true;
    if (other == null)
      return false;
    if (!(other instanceof DurableClientAttributes))
      return false;
    final DurableClientAttributes that = (DurableClientAttributes) other;

    if (this.timeout != that.getTimeout())
      return false;
    if (!(this.id != null && this.id.equals(that.id)
        && ((this.poolName == null && that.poolName == null)
            || (this.poolName != null && this.poolName.equals(that.poolName)))))
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = 17;
    final int mult = 37;

    result = mult * result + this.timeout;
    result = mult * result + (this.id == null ? 0 : this.getId().hashCode());

    return result;
  }

  @Override
  public String toString() {
    StringBuffer buffer = new StringBuffer(50).append("DurableClientAttributes[id=")
        .append(this.getId()).append("; timeout=").append(this.timeout).append("]");
    return buffer.toString();
  }
}
