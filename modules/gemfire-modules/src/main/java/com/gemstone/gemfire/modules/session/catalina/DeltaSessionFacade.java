/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.modules.session.catalina;

import org.apache.catalina.session.StandardSessionFacade;

import javax.servlet.http.HttpSession;

public class DeltaSessionFacade extends StandardSessionFacade {
  
  private DeltaSession session;

  /**
   * Construct a new session facade.
   */
  public DeltaSessionFacade(DeltaSession session) {
    super((HttpSession)session);
    // Store session locally since the super session is private and provides no accessor.
    this.session = session;
  }

  // ----------- DeltaSession Methods
  
  public void commit() {
    this.session.commit();
  }
  
  public void abort() {
    this.session.abort();
  }

  boolean isValid() {
    return this.session.isValid();
  }
}
