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

/**
 * Enumeration of all possible event types which can be listened for.
 */
public enum ListenerEventType {

  /**
   * HttpSessionAttributeListener
   */
  SESSION_ATTRIBUTE_ADDED,
  SESSION_ATTRIBUTE_REMOVED,
  SESSION_ATTRIBUTE_REPLACED,

  /**
   * HttpSessionBindingListener
   */
  SESSION_VALUE_BOUND,
  SESSION_VALUE_UNBOUND,

  /**
   * HttpSessionListener
   */
  SESSION_CREATED,
  SESSION_DESTROYED,

  /**
   * HttpSessionActivationListener
   */
  SESSION_WILL_ACTIVATE,
  SESSION_DID_PASSIVATE,

  /**
   * ServletContextListener
   */
  SERVLET_CONTEXT_INITIALIZED,
  SERVLET_CONTEXT_DESTROYED,

  /**
   * ServletContextAttributeListener
   */
  SERVLET_CONTEXT_ATTRIBUTE_ADDED,
  SERVLET_CONTEXT_ATTRIBUTE_REMOVED,
  SERVLET_CONTEXT_ATTRIBUTE_REPLACED,

  /**
   * ServletRequestListener
   */
  SERVLET_REQUEST_DESTROYED,
  SERVLET_REQUEST_INITIALIZED,

  /**
   * ServletRequestAttributeListener
   */
  SERVLET_REQUEST_ATTRIBUTE_ADDED,
  SERVLET_REQUEST_ATTRIBUTE_REMOVED,
  SERVLET_REQUEST_ATTRIBUTE_REPLACED;
}
