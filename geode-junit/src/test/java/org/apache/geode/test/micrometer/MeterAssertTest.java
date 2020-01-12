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

import static java.util.Arrays.asList;
import static org.apache.geode.test.micrometer.MicrometerAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import org.junit.Test;

public class MeterAssertTest {
  private final Meter meter = mock(Meter.class, RETURNS_DEEP_STUBS);

  @Test
  public void hasName_doesNotThrow_ifMeterHasGivenName() {
    String expectedName = "expected-name";

    when(meter.getId().getName()).thenReturn(expectedName);

    assertThatCode(() -> assertThat(meter).hasName(expectedName))
        .doesNotThrowAnyException();
  }

  @Test
  public void hasName_failsDescriptively_ifMeterDoesNotHaveGivenName() {
    String expectedName = "expected-name";
    String actualName = "actual-name";

    when(meter.getId().getName()).thenReturn(actualName);

    assertThatThrownBy(() -> assertThat(meter).hasName(expectedName))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(expectedName)
        .hasMessageContaining(actualName);
  }

  @Test
  public void hasTagWithKey_doesNotThrow_ifMeterHasTagWithGivenKey() {
    String expectedKey = "expected-key";

    when(meter.getId().getTag(expectedKey)).thenReturn("some-non-null-value");

    assertThatCode(() -> assertThat(meter).hasTag(expectedKey))
        .doesNotThrowAnyException();
  }

  @Test
  public void hasTagWithKey_failsDescriptively_ifMeterHasNoTagWithGivenKey() {
    String expectedKey = "expected-key";

    when(meter.getId().getTag(expectedKey)).thenReturn(null);

    Tag actualTag1 = mock(Tag.class, "actual-tag-1");
    Tag actualTag2 = mock(Tag.class, "actual-tag-2");
    when(meter.getId().getTags()).thenReturn(asList(actualTag1, actualTag2));

    assertThatThrownBy(() -> assertThat(meter).hasTag(expectedKey))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(expectedKey)
        .hasMessageContaining(actualTag1.toString())
        .hasMessageContaining(actualTag2.toString());
  }

  @Test
  public void hasTagWithKeyAndValue_doesNotThrow_ifMeterHasTagWithGivenKeyAndValue() {
    String expectedKey = "expected-key";
    String expectedValue = "expected-value";

    when(meter.getId().getTag(expectedKey)).thenReturn(expectedValue);

    assertThatCode(() -> assertThat(meter).hasTag(expectedKey, expectedValue))
        .doesNotThrowAnyException();
  }

  @Test
  public void hasTagWithKeyAndValue_failsDescriptively_ifMeterHasNoTagWithGivenKey() {
    String expectedKey = "expected-key";

    when(meter.getId().getTag(expectedKey)).thenReturn(null);

    Tag actualTag1 = mock(Tag.class, "actual-tag-1");
    Tag actualTag2 = mock(Tag.class, "actual-tag-2");
    when(meter.getId().getTags()).thenReturn(asList(actualTag1, actualTag2));

    assertThatThrownBy(() -> assertThat(meter).hasTag(expectedKey, "expected-value"))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(expectedKey)
        .hasMessageContaining(actualTag1.toString())
        .hasMessageContaining(actualTag2.toString());
  }

  @Test
  public void hasTagWithKeyAndValue_failsDescriptively_ifMeterHasWrongValueForTagWithGivenKey() {
    String expectedKey = "expected-key";
    String expectedValue = "expected-value";
    String wrongValue = "actual-value";

    when(meter.getId().getTag(expectedKey)).thenReturn(wrongValue);

    assertThatThrownBy(() -> assertThat(meter).hasTag(expectedKey, expectedValue))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(expectedKey)
        .hasMessageContaining(expectedValue)
        .hasMessageContaining(wrongValue);
  }

  @Test
  public void hasNoTagWithKey_failsDescriptively_ifMeterHasTagWithGivenKey() {
    String unexpectedKey = "unexpected-key";
    String unexpectedValue = "value-for-unexpected-key";

    when(meter.getId().getTag(unexpectedKey)).thenReturn(unexpectedValue);

    assertThatThrownBy(() -> assertThat(meter).hasNoTag(unexpectedKey))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(unexpectedKey)
        .hasMessageContaining(unexpectedValue);
  }

  @Test
  public void hasNoTagWithKey_doesNotThrow_ifMeterHasNoTagWithGivenKey() {
    String unexpectedKey = "unexpected-key";

    when(meter.getId().getTag(unexpectedKey)).thenReturn(null);

    assertThatCode(() -> assertThat(meter).hasNoTag(unexpectedKey))
        .doesNotThrowAnyException();
  }

  @Test
  public void hasBaseUnit_doesNotThrow_ifMeterHasGivenBaseUnit() {
    String expectedBaseUnit = "expected-base_unit";

    when(meter.getId().getBaseUnit()).thenReturn(expectedBaseUnit);

    assertThatCode(() -> assertThat(meter).hasBaseUnit(expectedBaseUnit))
        .doesNotThrowAnyException();
  }

  @Test
  public void hasBaseUnit_failsDescriptively_ifMeterDoesNotHaveGivenBaseUnit() {
    String expectedBaseUnit = "expected-base-unit";
    String wrongBaseUnit = "actual-base-unit";

    when(meter.getId().getBaseUnit()).thenReturn(wrongBaseUnit);

    assertThatThrownBy(() -> assertThat(meter).hasBaseUnit(expectedBaseUnit))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(expectedBaseUnit)
        .hasMessageContaining(wrongBaseUnit);
  }
}
