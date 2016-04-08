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
package com.gemstone.gemfire.test.dunit;

/**
 * Extends <code>org.junit.Assert</code> with additional assertion and fail
 * methods. 
 * 
 * These methods can be used directly: <code>Assert.assertEquals(...)</code>, 
 * however, they are intended to be referenced through static import:
 *
 * <pre>
 * import static com.gemstone.gemfire.test.dunit.Assert.*;
 *    ...
 *    fail(...);
 * </pre>
 *
 * Extracted from DistributedTestCase.
 * 
 * @see java.lang.AssertionError
 */
public class Assert extends org.junit.Assert {

  protected Assert() {
  }

  /**
   * Fails a test by throwing a new {@code AssertionError} with the specified
   * detail message and cause.
   *
   * <p>Note that the detail message associated with
   * {@code cause} is <i>not</i> automatically incorporated in
   * this error's detail message.
   *
   * @param  message the detail message, may be {@code null}
   * @param  cause the cause, may be {@code null}
   *
   * @see java.lang.AssertionError
   */
  public static void fail(final String message, final Throwable cause) {
    if (message == null && cause == null) {
      throw new AssertionError();
    }    
    if (message == null) {
      throw new AssertionError(cause);
    }
    if (cause == null) {
      throw new AssertionError(message);
    }
    throw new AssertionError(message, cause);
  }
}
