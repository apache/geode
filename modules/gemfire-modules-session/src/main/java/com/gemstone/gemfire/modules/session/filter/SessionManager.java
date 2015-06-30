/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.modules.session.filter;

import javax.servlet.http.HttpSession;

/**
 * Interface to session management. This class would be responsible for creating
 * new sessions.
 */
public interface SessionManager {

  /**
   * Start the manager possibly using the config passed in.
   *
   * @param config Config object specific to individual implementations.
   * @param loader This is a hack. When the manager is started it wants to be
   *               able to determine if the cache, which it would create, and
   *               the filter which starts everything, are defined by the same
   *               classloader. This is so that during shutdown, the manager can
   *               decide whether or not to also stop the cache. This option
   *               allows the filter's classloader to be passed in.
   */
  public void start(Object config, ClassLoader loader);

  /**
   * Stop the session manager and free up any resources.
   */
  public void stop();

  /**
   * Write the session to the region
   *
   * @param session the session to write
   */
  public void putSession(HttpSession session);

  /**
   * Return a session if it exists or null otherwise
   *
   * @param id The session id to attempt to retrieve
   * @return a HttpSession object if a session was found otherwise null.
   */
  public HttpSession getSession(String id);

  /**
   * Create a new session, wrapping a container session.
   *
   * @param nativeSession
   * @return the HttpSession object
   */
  public HttpSession wrapSession(HttpSession nativeSession);

  /**
   * Get the wrapped (GemFire) session from a native session id. This method
   * would typically be used from within session/http event listeners which
   * receive the original session id.
   *
   * @param nativeId
   * @return the wrapped GemFire session which maps the native session
   */
  public HttpSession getWrappingSession(String nativeId);

  /**
   * Destroy the session associated with the given id.
   *
   * @param id The id of the session to destroy.
   */
  public void destroySession(String id);

  /**
   * Destroy the session associated with a given native session
   *
   * @param id the id of the native session
   * @return the corresponding Gemfire session which wrapped the native session
   * and was destroyed.
   */
  public String destroyNativeSession(String id);

  /**
   * Returns the cookie name used to hold the session id. By default this is
   * JSESSIONID.
   *
   * @return the name of the cookie which contains the session id
   */
  public String getSessionCookieName();

  /**
   * Get the JVM Id - this is a unique string used internally to identify who
   * last touched a session.
   *
   * @return the jvm id
   */
  public String getJvmId();
}
