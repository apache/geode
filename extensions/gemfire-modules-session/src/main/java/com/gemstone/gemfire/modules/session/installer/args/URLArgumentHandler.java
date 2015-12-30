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
