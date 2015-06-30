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

import com.gemstone.gemfire.modules.session.filter.ListenerEventType;

import javax.servlet.http.HttpSessionEvent;
import javax.servlet.http.HttpSessionListener;

/**
 * @author jdeppe
 */
public class HttpSessionListenerImpl extends AbstractListener
    implements HttpSessionListener {

  public synchronized void sessionCreated(HttpSessionEvent se) {
    events.add(ListenerEventType.SESSION_CREATED);
    latch.countDown();
  }

  public synchronized void sessionDestroyed(HttpSessionEvent se) {
    events.add(ListenerEventType.SESSION_DESTROYED);
    latch.countDown();
  }
}
