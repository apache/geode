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

import org.eclipse.jetty.servlet.ServletTester;

/**
 * Extend the base ServletTester class with a couple of helper methods. This
 * depends on a patched ServletTester class which exposes the _server variable
 * as package-private.
 */
public class MyServletTester extends ServletTester {

  public boolean isStarted() {
//    return _server.isStarted();
    return false;
  }

  public boolean isStopped() {
//    return _server.isStopped();
    return false;
  }
}
