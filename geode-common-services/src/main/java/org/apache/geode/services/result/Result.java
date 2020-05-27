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
package org.apache.geode.services.result;

import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.geode.annotations.Experimental;

/**
 * The {@link Result} type is an attempt at the function construct of
 * * <a href="https://www.vavr.io/vavr-docs/#_either">Either</a>. In this implementation a
 * {@link Result}
 * * can define either success or failure (error) using the same type.
 *
 * @param <SuccessType> the return type in the event of operational success
 * @param <FailureType> the return type in the event of operational failure
 */
@Experimental
public interface Result<SuccessType, FailureType> {
  /**
   * A mapping function that maps to either <SuccessType> or <FailureType> depending on success or
   * failure of the operation.
   *
   * @param successFunction the mapping function to map the SuccessType to the resultant type
   * @param errorFunction the mapping function to map the FailureType to the resultant error type
   * @param <T> the resultant type
   * @return result of type <T>
   */
  <T> T map(Function<SuccessType, T> successFunction,
      Function<FailureType, T> errorFunction);

  /**
   * The return message of a successful operation. The return type is of type <SuccessType>
   *
   * @return the result of the operation
   */
  SuccessType getMessage();

  /**
   * The return message of a failed operation. The return type is of type <FailureType>
   *
   * @return the failure message of why the operation did not succeed.
   */
  FailureType getErrorMessage();

  /**
   * Returns a boolean to indicate the success or failure of the operation
   *
   * @return {@literal true} or {@literal false} indicating success or failure of the operation
   */
  default boolean isSuccessful() {
    return false;
  }

  /**
   * If the result of the operation is successful, invoke the specified consumer with the value,
   * otherwise do nothing.
   *
   * @param consumer block to be executed if a value is present
   * @throws NullPointerException if value is present and {@code consumer} is
   *         null
   */
  default void ifSuccessful(Consumer<? super SuccessType> consumer) {}

  /**
   * If the result of the operation has failed, invoke the specified consumer with the value,
   * otherwise do nothing.
   *
   * @param consumer block to be executed if a value is present
   * @throws NullPointerException if value is present and {@code consumer} is
   *         null
   */
  default void ifFailure(Consumer<? super String> consumer) {}
}
