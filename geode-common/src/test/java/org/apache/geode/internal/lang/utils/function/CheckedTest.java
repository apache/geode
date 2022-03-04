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

import static org.apache.geode.internal.lang.utils.function.Checked.rethrowFunction;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class CheckedTest {

  @Test
  void rethrowFunctionThrowsCheckedException() {
    assertThatThrownBy(() -> rethrowFunction(t -> {
      throw new Exception();
    }).apply(null)).isExactlyInstanceOf(Exception.class);
  }

  @Test
  void rethrowFunctionReturnsResults() {
    final Object o = new Object();
    assertThat(rethrowFunction(t -> o).apply(null)).isSameAs(o);
  }

}
