/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.gemstone.gemfire.modules.session.filter;

import javax.servlet.http.HttpSession;
import javax.servlet.http.HttpSessionEvent;
import javax.servlet.http.HttpSessionListener;

/**
 * @author jdeppe
 */
public class HttpSessionListenerImpl extends AbstractListener
    implements HttpSessionListener {

  @Override
  public void sessionCreated(HttpSessionEvent se) {
    HttpSession gfeSession = SessionCachingFilter.getWrappingSession(
        se.getSession());
    gfeSession.setAttribute("gemfire-session-id", gfeSession.getId());
    events.add(ListenerEventType.SESSION_CREATED);
    latch.countDown();
  }

  @Override
  public void sessionDestroyed(HttpSessionEvent se) {
    events.add(ListenerEventType.SESSION_DESTROYED);
    latch.countDown();
  }
}
