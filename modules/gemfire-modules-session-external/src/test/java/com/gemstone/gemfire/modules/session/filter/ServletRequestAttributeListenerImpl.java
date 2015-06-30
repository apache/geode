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

import javax.servlet.ServletRequestAttributeEvent;
import javax.servlet.ServletRequestAttributeListener;

/**
 * @author jdeppe
 */
public class ServletRequestAttributeListenerImpl extends AbstractListener
    implements ServletRequestAttributeListener {

  public synchronized void attributeAdded(ServletRequestAttributeEvent srae) {
    events.add(ListenerEventType.SERVLET_REQUEST_ATTRIBUTE_ADDED);
    latch.countDown();
  }

  public synchronized void attributeRemoved(ServletRequestAttributeEvent srae) {
    events.add(ListenerEventType.SERVLET_REQUEST_ATTRIBUTE_REMOVED);
    latch.countDown();
  }

  public synchronized void attributeReplaced(
      ServletRequestAttributeEvent srae) {
    events.add(ListenerEventType.SERVLET_REQUEST_ATTRIBUTE_REPLACED);
    latch.countDown();
  }

}
