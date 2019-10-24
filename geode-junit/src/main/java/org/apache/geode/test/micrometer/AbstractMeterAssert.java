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
package org.apache.geode.test.micrometer;

import java.util.Objects;

import io.micrometer.core.instrument.Meter;
import org.assertj.core.api.AbstractAssert;

/**
 * Assertions applicable to all {@link Meter} types.
 *
 * @param <A> the concrete type of this assertion object
 * @param <M> the type of meter to evaluate
 */
public class AbstractMeterAssert<A extends AbstractMeterAssert<A, M>, M extends Meter>
    extends AbstractAssert<A, M> {

  private final Meter.Id meterId;

  /**
   * Creates an assertion to evaluate the given meter.
   *
   * @param meter the meter to evaluate
   * @param selfType the concrete type of the assertion object
   */
  AbstractMeterAssert(M meter, Class<A> selfType) {
    super(meter, selfType);
    meterId = meter.getId();
  }

  /**
   * Verifies that the meter has the given name.
   *
   * @param expectedName the expected name
   * @return this assertion object
   * @throws AssertionError if the meter is {@code null}
   * @throws AssertionError if the meter's name is not equal to the given name
   */
  public A hasName(String expectedName) {
    isNotNull();
    String meterName = meterId.getName();
    if (!Objects.equals(meterName, expectedName)) {
      failWithMessage("Expected meter to have name <%s> but name was <%s>", expectedName,
          meterName);
    }
    return myself;
  }

  /**
   * Verifies that the meter has a tag with the given key.
   *
   * @param key the expected tag key
   * @return this assertion object
   * @throws AssertionError if the meter is {@code null}
   * @throws AssertionError if the meter has no tag with the given key
   */
  public A hasTag(String key) {
    isNotNull();
    if (meterId.getTag(key) == null) {
      failWithMessage("Expected meter to have tag with key <%s>"
          + " but meter had no tag with that key in <%s>",
          key, meterId.getTags());
    }
    return myself;
  }

  /**
   * Verifies that the meter has a tag with the given key and value.
   *
   * @param key the tag key
   * @param expectedValue the expected value of the tag with the given key
   * @return this assertion object
   * @throws AssertionError if the meter is {@code null}
   * @throws AssertionError if the meter has no tag with the given key
   * @throws AssertionError if the tag with the given key does not have the given value
   */
  public A hasTag(String key, String expectedValue) {
    hasTag(key);
    String tagValue = meterId.getTag(key);
    if (!Objects.equals(tagValue, expectedValue)) {
      failWithMessage("Expected meter's <%s> tag to have value <%s>"
          + " but value was <%s>",
          key, expectedValue, tagValue);
    }
    return myself;
  }

  /**
   * Verifies that the meter has the given base unit.
   *
   * @param expectedBaseUnit the expected base unit
   * @return this assertion object
   * @throws AssertionError if the meter is {@code null}
   * @throws AssertionError if the meter does not have the given base unit
   */
  public A hasBaseUnit(String expectedBaseUnit) {
    isNotNull();
    String meterBaseUnit = meterId.getBaseUnit();
    if (!Objects.equals(meterBaseUnit, expectedBaseUnit)) {
      failWithMessage("Expected meter to have base unit <%s> but base unit was <%s>",
          expectedBaseUnit, meterBaseUnit);
    }
    return myself;
  }
}
