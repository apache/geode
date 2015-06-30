/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.modules.session.filter;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Test;
import junit.framework.TestSuite;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * The goal of having a suite of suites is for each suite to be able to start up
 * and shut down a unique cache with a different config. Currently a restart
 * doesn't work... This is dependent on gemfire 6.5.1.4 which provides a unified
 * classloading framework. Once that can be introduced here we can simply switch
 * the URLClassLoader for a ChildFirstClassLoader and have the ability to run
 * multiple caches in a single JVM. This should also allow us the ability to
 * cleanly shut down a cache at the end of a test.
 * <p/>
 * To be continued...
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
  SessionTestSuite1.class,
  SessionTestSuite2.class
})
public class SessionUberSuite {
}