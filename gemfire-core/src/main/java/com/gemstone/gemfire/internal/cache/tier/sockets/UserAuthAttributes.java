/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.tier.sockets;

import java.util.concurrent.atomic.AtomicInteger;

import com.gemstone.gemfire.internal.security.AuthorizeRequest;
import com.gemstone.gemfire.internal.security.AuthorizeRequestPP;

public class UserAuthAttributes
{
  private AtomicInteger numberOfDurableCQ;

  /**
   * Authorize client requests using this object. This is set when each
   * operation on this connection is authorized in pre-operation phase.
   */
  private AuthorizeRequest authzRequest;

  /**
   * Authorize client requests using this object. This is set when each
   * operation on this connection is authorized in post-operation phase.
   */
  private AuthorizeRequestPP postAuthzRequest;
  
  public UserAuthAttributes(AuthorizeRequest authzRequest, AuthorizeRequestPP postAuthzRequest) {
    this.authzRequest = authzRequest;
    this.postAuthzRequest = postAuthzRequest;
    this.numberOfDurableCQ = new AtomicInteger();    
  }
  
  public AuthorizeRequest getAuthzRequest() {
    return this.authzRequest;
  }

  public AuthorizeRequestPP getPostAuthzRequest() {
    return this.postAuthzRequest;
  }
  /*
  public void setDurable(boolean isDurable) {
    if(isDurable)
    {
      this.numberOfDurableCQ.incrementAndGet();
    }
  }
  */
  public void setDurable() {
    this.numberOfDurableCQ.incrementAndGet();
  }
  
  public void unsetDurable() {
    this.numberOfDurableCQ.decrementAndGet();
  }
  
  
  public boolean isDurable() {
    return this.numberOfDurableCQ.intValue() != 0;
  }
  /*protected void setAuthorizeRequest(AuthorizeRequest authzRequest) {
    this.authzRequest = authzRequest;
  }

  protected void setPostAuthorizeRequest(AuthorizeRequestPP postAuthzRequest) {
    this.postAuthzRequest = postAuthzRequest;
  }*/

}
