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

import java.io.Serializable;
import javax.servlet.http.HttpSessionBindingEvent;
import javax.servlet.http.HttpSessionBindingListener;

/**
 * @author jdeppe
 */
public class HttpSessionBindingListenerImpl extends AbstractListener implements
    HttpSessionBindingListener, Serializable {

  public HttpSessionBindingListenerImpl(int i) {
    super(i);
  }

  @Override
  public synchronized void valueBound(HttpSessionBindingEvent event) {
    events.add(ListenerEventType.SESSION_VALUE_BOUND);
    latch.countDown();
  }

  @Override
  public synchronized void valueUnbound(HttpSessionBindingEvent event) {
    events.add(ListenerEventType.SESSION_VALUE_UNBOUND);
    latch.countDown();
  }
}
