/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package com.gemstone.gemfire.modules.session.internal.filter;

import com.gemstone.gemfire.modules.session.internal.filter.attributes.AbstractSessionAttributes;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import javax.servlet.http.HttpSession;

/**
 * Class which fakes an in-memory basic session manager for testing purposes.
 */
public class DummySessionManager implements SessionManager {

  /**
   * Map of sessions
   */
  private final Map<String, HttpSession> sessions =
      new HashMap<String, HttpSession>();

  private class Attributes extends AbstractSessionAttributes {

    @Override
    public Object putAttribute(String attr, Object value) {
      return attributes.put(attr, value);
    }

    @Override
    public Object removeAttribute(String attr) {
      return attributes.remove(attr);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void start(Object config, ClassLoader loader) {
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void stop() {
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public HttpSession getSession(String id) {
    return sessions.get(id);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public HttpSession wrapSession(HttpSession nativeSession) {
    String id = generateId();
    AbstractSessionAttributes attributes = new Attributes();
    GemfireHttpSession session = new GemfireHttpSession(id, nativeSession);
    session.setManager(this);
    session.setAttributes(attributes);
    sessions.put(id, session);

    return session;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public HttpSession getWrappingSession(String nativeId) {
    return sessions.get(nativeId);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void putSession(HttpSession session) {
    // shouldn't ever get called
    throw new UnsupportedOperationException("Not supported yet.");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void destroySession(String id) {
    sessions.remove(id);
  }

  @Override
  public String destroyNativeSession(String id) {
    return null;
  }

  public String getSessionCookieName() {
    return "JSESSIONID";
  }

  public String getJvmId() {
    return "jvm-id";
  }

  /**
   * Generate an ID string
   */
  private String generateId() {
    return UUID.randomUUID().toString().toUpperCase() + "-GF";
  }
}
