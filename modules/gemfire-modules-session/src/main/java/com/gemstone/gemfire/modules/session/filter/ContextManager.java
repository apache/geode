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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.servlet.ServletContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is a singleton and manages the mapping of context paths to
 * original ServletContext objects as well as a mapping of paths to Cache
 * reference handles.
 */
public class ContextManager {

  /**
   * Logger object
   */
  private static final Logger LOG =
      LoggerFactory.getLogger(ContextManager.class.getName());

  /**
   * Mapping of context path to ServletContext object
   */
  private final ConcurrentMap<String, ServletContext> contextMap =
      new ConcurrentHashMap<String, ServletContext>(16);

  /**
   * Our singleton reference
   */
  private static final ContextManager manager = new ContextManager();

  private ContextManager() {
    // This is a singleton
    LOG.info("Initializing ContextManager");
  }

  /**
   * Return our singleton instance
   *
   * @return
   */
  public static ContextManager getInstance() {
    return manager;
  }

  /**
   * Add a context to our collection
   *
   * @param context the {@code ServletContext} to add
   */
  public void putContext(ServletContext context) {
    String path = context.getContextPath();
    contextMap.put(path, context);
    LOG.info("Adding context '{}' {}", path, context);
  }

  /**
   * Remove a context from our collection
   *
   * @param context the context to remove
   */
  public void removeContext(ServletContext context) {
    String path = context.getContextPath();
    contextMap.remove(path);
    LOG.info("Removing context '{}'", path);
  }

  /**
   * Retrieve the context for a given path
   *
   * @param path
   * @return the GemfireServletContext object or null if the path is not found
   */
  public ServletContext getContext(String path) {
    ServletContext ctx = contextMap.get(path);
    if (ctx == null) {
      LOG.warn("No context for requested contextPath '{}'", path);
    }
    return ctx;
  }
}
