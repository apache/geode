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

/**
 * Invalid usage exception.
 */
public class UsageException extends Exception {

  /**
   * Serial format version.
   */
  private static final long serialVersionUID = 1L;

  /**
   * Stored usage message.
   */
  private String usage;

  /**
   * Creates a new UsageException.
   */
  public UsageException() {
    super();
  }

  /**
   * Creates a new UsageException.
   *
   * @param message description of exceptional condition
   */
  public UsageException(final String message) {
    super(message);
  }

  /**
   * Creates a new UsageException.
   *
   * @param message description of exceptional condition
   * @param cause   provoking exception
   */
  public UsageException(final String message, final Throwable cause) {
    super(message, cause);
  }

  /**
   * Creates a new UsageException.
   *
   * @param cause provoking exception
   */
  public UsageException(final Throwable cause) {
    super(cause);
  }


  /**
   * Attaches a usage message to the exception for later consumption.
   *
   * @param usageText text to display to user to guide them to correct usage.
   *                  This is generated and set by the <code>ArgsProcessor</code>.
   */
  public void setUsage(final String usageText) {
    usage = usageText;
  }

  /**
   * Returns the usage message previously set.
   *
   * @return message or null if not set.
   */
  public String getUsage() {
    return usage;
  }
}
