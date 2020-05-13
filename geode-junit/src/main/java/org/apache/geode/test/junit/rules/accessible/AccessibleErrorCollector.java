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
package org.apache.geode.test.junit.rules.accessible;

import static java.util.Objects.requireNonNull;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.List;

import org.junit.rules.ErrorCollector;

/**
 * Increases visibility of {@link #verify()} to public and uses reflection to acquire access to
 * the {@code List} of {@code Throwable}s in {@link ErrorCollector}.
 */
public class AccessibleErrorCollector extends ErrorCollector {

  private final List<Throwable> visibleErrors;

  public AccessibleErrorCollector() {
    visibleErrors = getErrorsReference();
  }

  @Override
  public void verify() throws Throwable {
    super.verify();
  }

  @Override
  public void addError(Throwable error) {
    super.addError(requireNonNull(error));
  }

  public List<Throwable> errors() {
    return visibleErrors;
  }

  public void addErrors(Collection<Throwable> errors) {
    visibleErrors.addAll(errors);
  }

  private List<Throwable> getErrorsReference() {
    try {
      Field superErrors = ErrorCollector.class.getDeclaredField("errors");
      superErrors.setAccessible(true);
      return (List<Throwable>) superErrors.get(this);
    } catch (IllegalAccessException | NoSuchFieldException e) {
      throw new RuntimeException(e);
    }
  }
}
