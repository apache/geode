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
package org.apache.geode.management.internal.cli.result;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;

import java.io.Serializable;

import org.apache.commons.lang3.SerializationUtils;
import org.junit.Test;

import org.apache.geode.management.cli.Result;

public class CommandResultExceptionTest {

  @Test
  public void isSerializable() {
    assertThat(CommandResultException.class).isInstanceOf(Serializable.class);
  }

  /**
   * Note: Result is NOT Serializable and so it must be transient to avoid failure.
   */
  @Test
  public void serializesWithoutThrowingNotSerializableException() {
    Result result = mock(Result.class);
    CommandResultException instance = new CommandResultException(result);

    assertThatCode(() -> SerializationUtils.clone(instance))
        .doesNotThrowAnyException();
  }

  /**
   * Note: Result is NOT Serializable and so it must be transient to avoid failure.
   */
  @Test
  public void serializesWithoutResult() {
    Result result = mock(Result.class);
    CommandResultException instance = new CommandResultException(result);

    CommandResultException cloned = SerializationUtils.clone(instance);

    assertThat(cloned.getResult()).isNull();
  }
}
