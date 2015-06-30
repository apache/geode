/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.modules.session.common;

import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;

import java.util.Properties;
import javax.servlet.http.HttpSession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SessionExpirationCacheListener extends
    CacheListenerAdapter<String, HttpSession> implements Declarable {

  private static final Logger LOG =
      LoggerFactory.getLogger(SessionExpirationCacheListener.class.getName());

  @Override
  public void afterDestroy(EntryEvent<String, HttpSession> event) {
    /**
     * A Session expired. If it was destroyed by GemFire expiration,
     * process it. If it was destroyed via Session.invalidate, ignore it
     * since it has already been processed.
     */
    if (event.getOperation() == Operation.EXPIRE_DESTROY) {
      HttpSession session = (HttpSession) event.getOldValue();
      session.invalidate();
    }
  }

  @Override
  public void init(Properties p) {
  }
}
