/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.modules.session.installer.args;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Argument handler implementation which accepts file paths or URLs and
 * normalizes the parameters to URLs.
 */
public class URLArgumentHandler implements ArgumentHandler {

  /**
   * Logger.
   */
  private static final Logger LOG =
      Logger.getLogger(URLArgumentHandler.class.getName());

  /**
   * Ensure that the argument is either a file path or a properly formatted URL.
   *  If it is a file path, convert to a URL.  If neither, throws a
   * UsageException.
   *
   * @param arg        argument
   * @param form       form used
   * @param parameters parameters supplied
   * @throws UsageException when file not found or not a workable URL
   */
  public void handleArgument(
      final Argument arg,
      final String form,
      final String[] parameters)
      throws UsageException {
    final File file = new File(parameters[0]);
    URL result = null;

    if (file.exists()) {
      try {
        result = file.toURI().toURL();
      } catch (MalformedURLException mux) {
        LOG.log(Level.FINEST, "Caught Exception", mux);
      }
    }
    if (result == null) {
      try {
        result = new URL(parameters[0]);
      } catch (MalformedURLException mux) {
        LOG.log(Level.FINEST, "Caught Exception", mux);
      }
    }
    if (result == null) {
      throw (new UsageException(
          "Argument parameter value is not a valid file "
              + "path or URL: " + arg));
    }
    parameters[0] = result.toString();
  }
}
