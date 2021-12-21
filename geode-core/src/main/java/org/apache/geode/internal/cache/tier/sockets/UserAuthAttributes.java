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
package org.apache.geode.internal.cache.tier.sockets;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.geode.internal.security.AuthorizeRequest;
import org.apache.geode.internal.security.AuthorizeRequestPP;

public class UserAuthAttributes {
  private final AtomicInteger numberOfDurableCQ;

  /**
   * Authorize client requests using this object. This is set when each operation on this connection
   * is authorized in pre-operation phase.
   */
  private final AuthorizeRequest authzRequest;

  /**
   * Authorize client requests using this object. This is set when each operation on this connection
   * is authorized in post-operation phase.
   */
  private final AuthorizeRequestPP postAuthzRequest;

  public UserAuthAttributes(AuthorizeRequest authzRequest, AuthorizeRequestPP postAuthzRequest) {
    this.authzRequest = authzRequest;
    this.postAuthzRequest = postAuthzRequest;
    numberOfDurableCQ = new AtomicInteger();
  }

  public AuthorizeRequest getAuthzRequest() {
    return authzRequest;
  }

  public AuthorizeRequestPP getPostAuthzRequest() {
    return postAuthzRequest;
  }

  /*
   * public void setDurable(boolean isDurable) { if(isDurable) {
   * this.numberOfDurableCQ.incrementAndGet(); } }
   */
  public void setDurable() {
    numberOfDurableCQ.incrementAndGet();
  }

  public void unsetDurable() {
    numberOfDurableCQ.decrementAndGet();
  }


  public boolean isDurable() {
    return numberOfDurableCQ.intValue() != 0;
  }
  /*
   * protected void setAuthorizeRequest(AuthorizeRequest authzRequest) { this.authzRequest =
   * authzRequest; }
   *
   * protected void setPostAuthorizeRequest(AuthorizeRequestPP postAuthzRequest) {
   * this.postAuthzRequest = postAuthzRequest; }
   */

}
