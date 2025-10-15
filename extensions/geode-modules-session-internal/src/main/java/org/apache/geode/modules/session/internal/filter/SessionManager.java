/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.geode.modules.session.internal.filter;

import jakarta.servlet.ServletContext;
import jakarta.servlet.http.HttpSession;

/**
 * Interface to session management. This class would be responsible for creating new sessions.
 */
public interface SessionManager {

  /**
   * Start the manager possibly using the config passed in.
   *
   * @param config Config object specific to individual implementations.
   * @param loader This is a hack. When the manager is started it wants to be able to determine if
   *        the cache, which it would create, and the filter which starts everything, are defined by
   *        the same classloader. This is so that during shutdown, the manager can decide whether or
   *        not to also stop the cache. This option allows the filter's classloader to be passed in.
   */
  void start(Object config, ClassLoader loader);

  /**
   * Stop the session manager and free up any resources.
   */
  void stop();

  /**
   * Write the session to the region
   *
   * @param session the session to write
   */
  void putSession(HttpSession session);

  /**
   * Return a session if it exists or null otherwise
   *
   * @param id The session id to attempt to retrieve
   * @return a HttpSession object if a session was found otherwise null.
   */
  HttpSession getSession(String id);

  /**
   * Create a new session, wrapping a container session.
   *
   * @return the HttpSession object
   */
  HttpSession wrapSession(ServletContext context, int maxInactiveInterval);

  /**
   * Destroy the session associated with the given id.
   *
   * @param id The id of the session to destroy.
   */
  void destroySession(String id);

  /**
   * Returns the cookie name used to hold the session id. By default this is JSESSIONID.
   *
   * @return the name of the cookie which contains the session id
   */
  String getSessionCookieName();

  /**
   * Get the JVM Id - this is a unique string used internally to identify who last touched a
   * session.
   *
   * @return the jvm id
   */
  String getJvmId();
}
