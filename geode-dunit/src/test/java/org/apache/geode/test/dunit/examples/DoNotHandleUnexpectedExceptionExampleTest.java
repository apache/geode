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
package org.apache.geode.test.dunit.examples;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

import org.junit.Test;

public class DoNotHandleUnexpectedExceptionExampleTest {

  @Test
  public void thisIsTooVerbose() {
    boolean success = false;
    try {
      success = doDangerousWork();
    } catch (Exception unexpected) {
      fail(unexpected.getMessage());
    }
    assertThat(success).isTrue();
  }

  @Test
  public void thisIsEvenWorse() {
    try {
      assertThat(doDangerousWork()).isTrue();
    } catch (Error | Exception unexpected) {
      fail(unexpected.getMessage());
    }
  }

  @Test
  public void thisIsTheCorrectWay() throws Exception {
    assertThat(doDangerousWork()).isTrue();
  }

  private boolean doDangerousWork() throws Exception {
    if (false) {
      throw new Exception("fatal");
    }
    return true;
  }
}
