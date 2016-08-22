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
package com.gemstone.gemfire.internal.lang;

/**
 * The ThrowableUtils class is an abstract utility class for working with {@code Throwable}s.
 * <p/>
 * @see java.lang.Throwable
 */
public abstract class ThrowableUtils {

  /**
   * Get the root cause of a {@link Throwable}. Returns the specified
   * {@code throwable} if its {@code getCause} returns null.
   *
   * @param  throwable the {@code Throwable} to get the root cause of.
   * @return the root cause of the specified {@code throwable}.
   *
   * @throws NullPointerException if {@code throwable} is null
   */
  public static Throwable getRootCause(Throwable throwable) {
    if (throwable.getCause() == null) {
      return throwable;
    }

    Throwable cause;
    while ((cause = throwable.getCause()) != null) {
      throwable = cause;
    }
    return throwable;
  }

  /**
   * Returns true if the {@link Throwable} or any of its causes as returned
   * by {@code getCause()} are an instance of the specified subclass of
   * {@code Throwable}.
   *
   * @param  throwable the {@code Throwable} to check the causes of.
   * @param  causeClass the subclass of {@code Throwable} to check for.
   * @return true if any cause of {@code throwable} is an instance of
   *         {@code causeClass}.
   *
   * @throws NullPointerException if {@code throwable} is null
   */
  public static boolean hasCauseType(Throwable throwable, Class<? extends Throwable> causeClass) {
    if (causeClass.isInstance(throwable)) {
      return true;
    }

    Throwable cause;
    while ((cause = throwable.getCause()) != null) {
      throwable = cause;
      if (causeClass.isInstance(throwable)) {
        return true;
      }
    }

    return false;
  }

  /**
   * Returns true if the {@link Throwable} or any of its causes contain the
   * specified {@code message}.
   *
   * @param  throwable the {@code Throwable} to check the causes of.
   * @param  message the {@code Throwable} message to check for.
   * @return true if any cause of {@code throwable} contains the specified
   *         {@code message}.
   *
   * @throws NullPointerException if {@code throwable} is null
   */
  public static boolean hasCauseMessage(Throwable throwable, String message) {
    if (throwable.getMessage().contains(message)) {
      return true;
    }

    Throwable cause;
    while ((cause = throwable.getCause()) != null) {
      throwable = cause;
      if (throwable.getMessage().contains(message)) {
        return true;
      }
    }

    return false;
  }
}
