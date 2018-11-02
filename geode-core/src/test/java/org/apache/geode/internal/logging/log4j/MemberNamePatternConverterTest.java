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
package org.apache.geode.internal.logging.log4j;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Unit tests for {@link MemberNamePatternConverter}.
 */
@Category(LoggingTest.class)
public class MemberNamePatternConverterTest {

  private MemberNamePatternConverter converter;
  private MemberNameSupplier supplier;
  private String name;
  private StringBuilder toAppendTo;

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() {
    converter = MemberNamePatternConverter.INSTANCE;
    supplier = converter.getMemberNameSupplier();
    name = testName.getMethodName();
    toAppendTo = new StringBuilder();
  }

  @Test
  public void appendsMemberName() {
    supplier.set(name);

    converter.format(null, toAppendTo);

    assertThat(toAppendTo.toString()).isEqualTo(name);
  }
}
