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

import javax.servlet.ServletRequestEvent;
import javax.servlet.ServletRequestListener;

/**
 * @author jdeppe
 */
public class ServletRequestListenerImpl extends AbstractListener
    implements ServletRequestListener {

  public synchronized void requestDestroyed(ServletRequestEvent sre) {
    events.add(ListenerEventType.SERVLET_REQUEST_DESTROYED);
    latch.countDown();
  }

  public synchronized void requestInitialized(ServletRequestEvent sre) {
    events.add(ListenerEventType.SERVLET_REQUEST_INITIALIZED);
    latch.countDown();
  }

}
