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
package org.apache.geode.management.internal.cli;

import static org.apache.geode.internal.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.management.cli.Result;
import org.apache.geode.test.junit.categories.UnitTest;

/**
 * AbstractCliAroundInterceptor Tester.
 */
@Category(UnitTest.class)
public class AbstractCliAroundInterceptorJUnitTest {

  @Test
  public void isNullOrEmpty()
  {
    AbstractCliAroundInterceptor interceptor = new AbstractCliAroundInterceptor() {
      @Override
      public Result preExecution(final GfshParseResult parseResult) {
        return null;
      }

      @Override
      public Result postExecution(final GfshParseResult parseResult, final Result commandResult) {
        return null;
      }
    };
    String empty = "";
    @SuppressWarnings("RedundantStringConstructorCall")
    String otherEmpty = new String(empty);  // create a new instance, not another reference

    //noinspection StringEquality
    assertFalse(empty == otherEmpty);
    assertTrue(interceptor.isNullOrEmpty(empty));
    assertTrue(interceptor.isNullOrEmpty(otherEmpty));
    assertTrue(interceptor.isNullOrEmpty(null));
    assertFalse(interceptor.isNullOrEmpty("not empty"));
  }
}
