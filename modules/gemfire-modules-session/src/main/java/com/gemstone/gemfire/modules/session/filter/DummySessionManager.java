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

import com.gemstone.gemfire.modules.session.filter.attributes.AbstractSessionAttributes;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
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