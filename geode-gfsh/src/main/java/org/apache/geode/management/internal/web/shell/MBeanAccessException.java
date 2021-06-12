/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.management.internal.web.shell;

/**
 * The MBeanAccessException class is a RuntimeException indicating that an attempt to access an
 * MBean attribute or invocation of an MBean operation failed.
 * <p/>
 *
 * @see java.lang.RuntimeException
 * @since GemFire 8.0
 */
@SuppressWarnings("unused")
public class MBeanAccessException extends RuntimeException {
  private static final long serialVersionUID = 813768898269516238L;

  public MBeanAccessException() {}

  public MBeanAccessException(final String message) {
    super(message);
  }

  public MBeanAccessException(final Throwable cause) {
    super(cause);
  }

  public MBeanAccessException(final String message, final Throwable cause) {
    super(message, cause);
  }

}
