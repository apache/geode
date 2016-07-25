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
   
package com.gemstone.gemfire;

/**
 * A <code>LicenseException</code> is thrown when
 * the license check fails.
 *
 * @deprecated Licensing is not supported as of 8.0.
 */
@Deprecated
public class LicenseException extends GemFireException {
private static final long serialVersionUID = -1178557127300465801L;

  //////////////////////  Constructors  //////////////////////

  /**
   * Creates a new <code>LicenseException</code>.
   */
  public LicenseException(String message) {
    super(message);
  }

  /**
   * Creates a new <code>LicenseException</code> that was
   * caused by a given exception
   */
  public LicenseException(String message, Throwable thr) {
    super(message, thr);
  }

  /**
   * Creates a new <code>LicenseException</code> that was
   * caused by a given exception
   */
  public LicenseException(Throwable thr) {
    super(thr.getMessage(), thr);
  }
}
