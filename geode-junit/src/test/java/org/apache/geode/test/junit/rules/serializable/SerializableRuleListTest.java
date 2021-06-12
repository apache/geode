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
package org.apache.geode.test.junit.rules.serializable;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;

import org.apache.commons.lang3.SerializationUtils;
import org.junit.Test;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;


/**
 * Unit tests for {@link SerializableRuleList}.
 */
public class SerializableRuleListTest {

  @Test
  public void isSerializable() throws Exception {
    assertThat(SerializableRuleList.class).isInstanceOf(Serializable.class);
  }

  @Test
  public void canBeSerialized() throws Exception {
    String value = "foo";
    FakeSerializableTestRule fakeRule = new FakeSerializableTestRule().value(value);
    SerializableRuleList instance = new SerializableRuleList().add(fakeRule);

    SerializableRuleList cloned = (SerializableRuleList) SerializationUtils.clone(instance);

    assertThat(cloned.rules().size()).isEqualTo(1);
    assertThat(cloned.rules().get(0)).isInstanceOf(FakeSerializableTestRule.class)
        .isEqualTo(fakeRule);
  }

  /**
   * Fake SerializableTestRule with a string field and overriding equals.
   */
  private static class FakeSerializableTestRule implements SerializableTestRule {

    private String value = null;

    public FakeSerializableTestRule value(final String value) {
      this.value = value;
      return this;
    }

    public String value() {
      return this.value;
    }

    @Override
    public Statement apply(final Statement base, final Description description) {
      return new Statement() {
        @Override
        public void evaluate() throws Throwable {
          base.evaluate();
        }
      };
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      FakeSerializableTestRule that = (FakeSerializableTestRule) o;

      return this.value != null ? this.value.equals(that.value()) : that.value() == null;
    }
  }
}
