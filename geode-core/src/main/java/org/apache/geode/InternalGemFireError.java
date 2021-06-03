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
package org.apache.geode;

/**
 * Indicates that serious error has occurred within the GemFire system.
 *
 * This is similar to {@link AssertionError}, but these errors are always enabled in a GemFire
 * system.
 *
 * @since GemFire 5.5
 * @see AssertionError
 */
public class InternalGemFireError extends Error {
  private static final long serialVersionUID = 6390043490679349593L;

  public InternalGemFireError() {
    // TODO Auto-generated constructor stub
  }

  public InternalGemFireError(String message) {
    super(message);
  }

  public InternalGemFireError(Throwable cause) {
    super(cause);
  }

  public InternalGemFireError(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Constructs an AssertionError with its detail message derived from the specified object, which
   * is converted to a string as defined in <i>The Java Language Specification, Second Edition</i>,
   * Section 15.18.1.1.
   * <p>
   * If the specified object is an instance of <tt>Throwable</tt>, it becomes the <i>cause</i> of
   * the newly constructed assertion error.
   *
   * @param detailMessage value to be used in constructing detail message
   * @see Throwable#getCause()
   */
  public InternalGemFireError(Object detailMessage) {
    this("" + detailMessage);
    if (detailMessage instanceof Throwable) {
      initCause((Throwable) detailMessage);
    }
  }

  /**
   * Constructs an AssertionError with its detail message derived from the specified
   * <code>boolean</code>, which is converted to a string as defined in <i>The Java Language
   * Specification, Second Edition</i>, Section 15.18.1.1.
   *
   * @param detailMessage value to be used in constructing detail message
   */
  public InternalGemFireError(boolean detailMessage) {
    this("" + detailMessage);
  }

  /**
   * Constructs an AssertionError with its detail message derived from the specified
   * <code>char</code>, which is converted to a string as defined in <i>The Java Language
   * Specification, Second Edition</i>, Section 15.18.1.1.
   *
   * @param detailMessage value to be used in constructing detail message
   */
  public InternalGemFireError(char detailMessage) {
    this("" + detailMessage);
  }

  /**
   * Constructs an AssertionError with its detail message derived from the specified
   * <code>int</code>, which is converted to a string as defined in <i>The Java Language
   * Specification, Second Edition</i>, Section 15.18.1.1.
   *
   * @param detailMessage value to be used in constructing detail message
   */
  public InternalGemFireError(int detailMessage) {
    this("" + detailMessage);
  }

  /**
   * Constructs an AssertionError with its detail message derived from the specified
   * <code>long</code>, which is converted to a string as defined in <i>The Java Language
   * Specification, Second Edition</i>, Section 15.18.1.1.
   *
   * @param detailMessage value to be used in constructing detail message
   */
  public InternalGemFireError(long detailMessage) {
    this("" + detailMessage);
  }

  /**
   * Constructs an AssertionError with its detail message derived from the specified
   * <code>float</code>, which is converted to a string as defined in <i>The Java Language
   * Specification, Second Edition</i>, Section 15.18.1.1.
   *
   * @param detailMessage value to be used in constructing detail message
   */
  public InternalGemFireError(float detailMessage) {
    this("" + detailMessage);
  }

  /**
   * Constructs an AssertionError with its detail message derived from the specified
   * <code>double</code>, which is converted to a string as defined in <i>The Java Language
   * Specification, Second Edition</i>, Section 15.18.1.1.
   *
   * @param detailMessage value to be used in constructing detail message
   */
  public InternalGemFireError(double detailMessage) {
    this("" + detailMessage);
  }

}
