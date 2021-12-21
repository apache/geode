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

import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.List;

import org.junit.rules.TestRule;

import org.apache.geode.test.junit.rules.RuleList;

/**
 * Serializable version of {@link RuleList}.
 */
public class SerializableRuleList extends RuleList implements SerializableTestRule {

  public SerializableRuleList() {
    super();
  }

  public SerializableRuleList(final TestRule rule) {
    super(rule);
  }

  protected SerializableRuleList(final List<TestRule> rules) {
    super(rules);
  }

  @Override
  public SerializableRuleList add(final TestRule rule) {
    super.add(rule);
    return this;
  }

  @Override
  protected List<TestRule> rules() {
    return super.rules();
  }

  private void readObject(final ObjectInputStream stream) throws InvalidObjectException {
    throw new InvalidObjectException("SerializationProxy required");
  }

  private Object writeReplace() {
    return new SerializationProxy(this);
  }

  /**
   * Serialization proxy for {@code SerializableRuleList}.
   */
  private static class SerializationProxy implements Serializable {

    private final List<TestRule> rules;

    SerializationProxy(final SerializableRuleList instance) {
      rules = instance.rules();
    }

    private Object readResolve() {
      return new SerializableRuleList(rules);
    }
  }
}
