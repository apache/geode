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

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

/**
 * @author jdeppe
 */
public class ServletContextListenerImpl extends AbstractListener
    implements ServletContextListener {

  /**
   * This is a 'custom' constructor which sets our latch count to 2. This is
   * done because the earliest point, within our test code, where we can get a
   * reference to the listener (in order to reset the latch count) an event
   * *may* (more than likely) already have been fired.
   */
  public ServletContextListenerImpl() {
    super(2);
  }

  @Override
  public void contextInitialized(ServletContextEvent sce) {
    events.add(ListenerEventType.SERVLET_CONTEXT_INITIALIZED);
    latch.countDown();
  }

  @Override
  public void contextDestroyed(ServletContextEvent sce) {
    events.add(ListenerEventType.SERVLET_CONTEXT_DESTROYED);
    latch.countDown();
  }

}
