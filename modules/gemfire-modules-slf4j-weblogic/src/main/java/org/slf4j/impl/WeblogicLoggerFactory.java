/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package org.slf4j.impl;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import weblogic.logging.NonCatalogLogger;

/**
 * Implementation of slf4j's ILoggerFactory interface
 */
public class WeblogicLoggerFactory implements ILoggerFactory {

  /**
   * A map of loggers
   */
  private Map<String, Logger> loggers;

  public WeblogicLoggerFactory() {
    loggers = new HashMap<String, Logger>();
  }

  @Override
  public Logger getLogger(String name) {
    Logger ulogger = null;

    synchronized (this) {
      ulogger = (Logger) loggers.get(name);
      if (ulogger == null) {
        NonCatalogLogger logger = new NonCatalogLogger(name);
        ulogger = new WeblogicLoggerAdapter(logger);
        loggers.put(name, ulogger);
      }
    }

    return ulogger;
  }
}
