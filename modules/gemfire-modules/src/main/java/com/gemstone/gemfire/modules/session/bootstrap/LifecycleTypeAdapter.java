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

package com.gemstone.gemfire.modules.session.bootstrap;

/**
 * Adapter for the Catalina Lifecycle event types
 */
public enum LifecycleTypeAdapter {

  CONFIGURE_START,

  CONFIGURE_STOP,

  AFTER_DESTROY,

  AFTER_INIT,

  AFTER_START,

  AFTER_STOP,

  BEFORE_DESTROY,

  BEFORE_INIT,

  BEFORE_START,

  BEFORE_STOP,

  DESTROY,

  INIT,

  PERIODIC,

  START,

  STOP;

}
