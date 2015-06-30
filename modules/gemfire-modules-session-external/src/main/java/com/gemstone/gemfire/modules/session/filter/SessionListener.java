/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.modules.session.filter;

import com.gemstone.gemfire.distributed.DistributedSystemDisconnectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpSessionEvent;
import javax.servlet.http.HttpSessionListener;

public class SessionListener implements HttpSessionListener {

  private static final Logger LOG =
      LoggerFactory.getLogger(SessionListener.class.getName());

  public void sessionCreated(HttpSessionEvent httpSessionEvent) {
  }

  /**
   * This will receive events from the container using the native sessions.
   */
  public void sessionDestroyed(HttpSessionEvent event) {
    String nativeId = event.getSession().getId();
    try {
      String sessionId = SessionCachingFilter.getSessionManager().destroyNativeSession(
          nativeId);
      LOG.debug(
          "Received sessionDestroyed event for native session {} (wrapped by {})",
          nativeId, sessionId);
    } catch (DistributedSystemDisconnectedException dex) {
      LOG.debug("Cache disconnected - unable to destroy native session {0}",
          nativeId);
    }
  }
}
