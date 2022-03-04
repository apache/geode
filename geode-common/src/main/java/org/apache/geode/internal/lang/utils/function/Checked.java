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

package org.apache.geode.internal.lang.utils.function;

import java.util.function.Function;

public class Checked {

  @FunctionalInterface
  public interface CheckedFunction<T, R, E extends Exception> {
    R apply(T t) throws E;
  }

  /**
   * Allows for the execution of a {@link Function} that throws checked exceptions. Uses trick to
   * rethrow a checked exception as unchecked.
   *
   * @param function to apply.
   * @param <T> the type of the input to the function
   * @param <R> the type of the result of the function
   * @param <E> the checked exception to be rethrown
   *
   * @return the function results
   */
  public static <T, R, E extends Exception> Function<T, R> rethrowFunction(
      CheckedFunction<T, R, E> function) {
    return t -> {
      try {
        return function.apply(t);
      } catch (Exception e) {
        throwAsUnchecked(e);
      }
      // never actually reaches here.
      return null;
    };
  }

  @SuppressWarnings("unchecked")
  private static <E extends Throwable> void throwAsUnchecked(Exception exception) throws E {
    throw (E) exception;
  }
}
