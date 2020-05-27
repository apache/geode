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

import org.apache.commons.lang3.StringUtils;

import org.apache.geode.services.result.ModuleServiceResult;

/**
 * This type of {@link ModuleServiceResult} represents a failed operation. It contains the
 * errorMessage
 * for the failure.
 *
 * @param <SuccessType> the result type for a successful operation. Not used by the {@link Failure}
 *        type
 *        but required by the {@link ModuleServiceResult}
 *
 * @since 1.14.0
 */
public class Failure<SuccessType> implements ModuleServiceResult<SuccessType> {

  private final String errorMessage;

  private Failure(String errorMessage) {
    this.errorMessage = errorMessage;
  }

  /**
   * Creates a {@link Failure} object containing the errorMessage
   *
   * @param errorMessage the error message describing the reason for failure.
   * @return an {@link Failure} instance containing the errorMessage
   */
  public static <T> Failure<T> of(String errorMessage) {
    if (StringUtils.isEmpty(errorMessage)) {
      throw new IllegalArgumentException("Error message cannot be null or empty");
    }
    return new Failure<>(errorMessage);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <T> T map(Function<SuccessType, T> successFunction, Function<String, T> errorFunction) {
    return errorFunction.apply(errorMessage);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public SuccessType getMessage() {
    throw new RuntimeException("This Result is not of type Success.");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getErrorMessage() {
    return errorMessage;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void ifFailure(Consumer<? super String> consumer) {
    if (!isSuccessful()) {
      consumer.accept(errorMessage);
    }
  }
}
