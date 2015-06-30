/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.modules.session.filter;

import javax.servlet.http.HttpSessionAttributeListener;
import javax.servlet.http.HttpSessionBindingEvent;

/**
 *
 */
public class HttpSessionAttributeListenerImpl extends AbstractListener
    implements HttpSessionAttributeListener {

  @Override
  public synchronized void attributeAdded(HttpSessionBindingEvent se) {
    events.add(ListenerEventType.SESSION_ATTRIBUTE_ADDED);
    latch.countDown();
  }

  @Override
  public synchronized void attributeRemoved(HttpSessionBindingEvent se) {
    events.add(ListenerEventType.SESSION_ATTRIBUTE_REMOVED);
    latch.countDown();
  }

  @Override
  public synchronized void attributeReplaced(HttpSessionBindingEvent se) {
    events.add(ListenerEventType.SESSION_ATTRIBUTE_REPLACED);
    latch.countDown();
  }
}
