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
package org.apache.geode.distributed.internal;

public class ServerLocationAndMemberId {

  private final ServerLocation serverLocation;
  private final String memberId;

  public ServerLocationAndMemberId(ServerLocation serverLocation, String memberId) {
    this.serverLocation = serverLocation;
    this.memberId = memberId;
  }

  public ServerLocation getServerLocation() {
    return this.serverLocation;
  }

  public String getMemberId() {
    return this.memberId;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof ServerLocationAndMemberId)) {
      return false;
    }
    final ServerLocationAndMemberId other = (ServerLocationAndMemberId) obj;

    if (!this.serverLocation.equals(other.getServerLocation())) {
      return false;
    }

    return this.memberId.equals(other.getMemberId());
  }

  @Override
  public String toString() {
    return serverLocation.toString() + "@" + memberId;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result =
        prime * result + serverLocation.hashCode() + memberId.hashCode();
    return result;
  }
}
