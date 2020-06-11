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

package org.apache.geode.services.result.impl;

import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.geode.services.result.ModuleServiceResult;

/**
 * This type of {@link ModuleServiceResult} represents a successful operation. It contains the
 * return value
 * of type <SuccessType>
 *
 * @param <SuccessType> the result type for a successful operation.
 *
 * @since 1.14.0
 */
public class Success<SuccessType> implements ModuleServiceResult<SuccessType> {

  private final SuccessType result;

  private Success(SuccessType result) {
    this.result = result;
  }

  /**
   * Creates a {@link Success} object containing the errorMessage
   *
   * @param result the return value of the successful operation
   * @return an {@link Success} instance containing the return value
   */
  public static <T> Success<T> of(T result) {
    return new Success<>(result);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <T> T map(Function<SuccessType, T> successFunction, Function<String, T> errorFunction) {
    return successFunction.apply(result);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public SuccessType getMessage() {
    return result;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getErrorMessage() {
    throw new RuntimeException("This Result is not of type Failure.");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isSuccessful() {
    return true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void ifSuccessful(Consumer<? super SuccessType> consumer) {
    if (isSuccessful()) {
      consumer.accept(result);
    }
  }
}
