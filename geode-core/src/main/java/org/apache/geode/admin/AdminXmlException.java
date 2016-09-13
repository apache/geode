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
package com.gemstone.gemfire.admin;

/**
 * Thrown when a problem is encountered while working with
 * admin-related XML data.
 *
 * @see DistributedSystemConfig#getEntityConfigXMLFile
 *
 * @since GemFire 4.0
 * @deprecated as of 7.0 use the <code><a href="{@docRoot}/com/gemstone/gemfire/management/package-summary.html">management</a></code> package instead
 */
public class AdminXmlException extends RuntimeAdminException {
  private static final long serialVersionUID = -6848726449157550169L;

  /**
   * Creates a new <code>AdminXmlException</code> with the given
   * descriptive message.
   */
  public AdminXmlException(String s) {
    super(s);
  }

  /**
   * Creates a new <code>AdminXmlException</code> with the given
   * descriptive message and cause.
   */
  public AdminXmlException(String s, Throwable cause) {
    super(s, cause);
  }

}
