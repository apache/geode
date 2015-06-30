/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.modules.session;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.modules.session.catalina.DeltaSessionManager;
import com.gemstone.gemfire.modules.session.catalina.PeerToPeerCacheLifecycleListener;
import com.gemstone.gemfire.modules.session.catalina.Tomcat7DeltaSessionManager;

import java.io.File;
import javax.servlet.http.HttpSession;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.catalina.core.StandardWrapper;

/**
 *
 */
public class AllTests extends TestSuite {

  static EmbeddedTomcat server;

  static Region<String, HttpSession> region;

  static StandardWrapper servlet;

  static DeltaSessionManager sessionManager;

  public static Test suite() {
    TestSuite suite = new TestSuite();

    suite.addTestSuite(TestSessions.class);
// This requires manual startup of a second cache...
//        suite.addTestSuite(DualCacheTest.class);

    TestSetup wrapper = new TestSetup(suite) {

      @Override
      public void setUp() throws Exception {
        setupClass();
      }

      @Override
      public void tearDown() throws Exception {
        teardownClass();
      }
    };

    return wrapper;
  }

  public static void setupClass() throws Exception {
    // Create a per-user scratch directory
    File tmpDir = new File(System.getProperty("java.io.tmpdir"),
        "gemfire_modules-" + System.getProperty("user.name"));
    tmpDir.mkdirs();
    tmpDir.deleteOnExit();

    String gemfireLog = tmpDir.getPath() +
        System.getProperty("file.separator") + "gemfire_modules.log";

    server = new EmbeddedTomcat("/test", 7890, "JVM-1");

    PeerToPeerCacheLifecycleListener p2pListener = new PeerToPeerCacheLifecycleListener();
    p2pListener.setProperty("mcast-port", "19991");
    p2pListener.setProperty("log-file", gemfireLog);
    p2pListener.setProperty("writable-working-dir", tmpDir.getPath());
    server.getEmbedded().addLifecycleListener(p2pListener);
    sessionManager = new Tomcat7DeltaSessionManager();
    server.getRootContext().setManager(sessionManager);

    servlet = server.addServlet("/test/*", "default",
        CommandServlet.class.getName());
    server.startContainer();

    /**
     * Can only retrieve the region once the container has started up
     * (and the cache has started too).
     */
    region = sessionManager.getSessionCache().getSessionRegion();
    Thread.sleep(5000);

  }

  public static void teardownClass() throws Exception {
    server.stopContainer();
    Thread.sleep(2000);
  }
}