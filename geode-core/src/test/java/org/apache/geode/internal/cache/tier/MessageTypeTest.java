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

package org.apache.geode.internal.cache.tier;

import static org.apache.geode.internal.cache.tier.MessageType.validate;
import static org.apache.geode.internal.cache.tier.MessageType.valueOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class MessageTypeTest {

  @Test
  void valueOfInvalidIdReturnsInvalidEnumValue() {
    assertThat(valueOf(-1)).isSameAs(MessageType.INVALID);
  }

  @Test
  void valueOfValidIdReturnsEnumValue() {
    assertThat(valueOf(0)).isSameAs(MessageType.REQUEST);
    assertThat(valueOf(42)).isSameAs(MessageType.EXECUTECQ);
  }

  @Test
  void valueOfGapIdWillThrowException() {
    assertThatThrownBy(() -> valueOf(57)).isInstanceOf(EnumConstantNotPresentException.class);
  }

  @Test
  void valueOfOutOfRangeIdThrowsException() {
    assertThatThrownBy(() -> valueOf(200)).isInstanceOf(EnumConstantNotPresentException.class);
    assertThatThrownBy(() -> valueOf(-200)).isInstanceOf(EnumConstantNotPresentException.class);
  }

  @Test
  void validateOutOfRangeIdReturnsFalse() {
    assertThat(validate(-200)).isFalse();
    assertThat(validate(200)).isFalse();
  }

  @Test
  void validateGapIdReturnsFalse() {
    assertThat(validate(57)).isFalse();
  }

  @Test
  void validateInvalidIdReturnsFalse() {
    assertThat(validate(-1)).isFalse();
  }

  @Test
  void validateValidIdReturnsTrue() {
    assertThat(validate(0)).isTrue();
    assertThat(validate(42)).isTrue();
  }

}
