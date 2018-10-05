/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.cache;

import static java.lang.Character.MAX_VALUE;
import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.internal.cache.RegionNameValidation.getNamePattern;
import static org.apache.geode.internal.cache.RegionNameValidation.validate;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.regex.Matcher;

import org.junit.Test;

public class RegionNameValidationTest {

  @Test
  public void nullThrows() {
    assertThatThrownBy(() -> validate(null)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("name cannot be null");
  }

  @Test
  public void emptyThrows() {
    assertThatThrownBy(() -> validate("")).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("name cannot be empty");
  }

  @Test
  public void endingWithSeparatorThrows() {
    assertThatThrownBy(() -> validate("foo" + SEPARATOR))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(String.format("name cannot contain the separator ' %s '",
            SEPARATOR));
  }

  @Test
  public void startingWithSeparatorThrows() {
    assertThatThrownBy(() -> validate(SEPARATOR + "foo"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(String.format("name cannot contain the separator ' %s '",
            SEPARATOR));
  }

  @Test
  public void containingSeparatorThrows() {
    assertThatThrownBy(() -> validate("foo" + SEPARATOR + "bar"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(String.format("name cannot contain the separator ' %s '",
            SEPARATOR));
  }

  @Test
  public void startingWithDoubleUnderscoreThrows() {
    assertThatThrownBy(() -> validate("__InvalidInternalRegionName"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Region names may not begin with a double-underscore");
  }

  @Test
  public void startingWithDoubleUnderscoreIsOkForInternalRegion() {
    InternalRegionArguments internalRegionArguments = new InternalRegionArguments();
    internalRegionArguments.setInternalRegion(true);

    assertThatCode(() -> validate("__ValidInternalRegionName", internalRegionArguments))
        .doesNotThrowAnyException();
  }

  @Test
  public void startingWithUnderscoreIsOk() {
    validate("_validRegionName");
  }

  @Test
  public void containingUnderscoreIsOk() {
    validate("valid_regionName");
  }

  @Test
  public void containingMultipleUnderscoresIsOk() {
    validate("valid_region_name");
  }

  @Test
  public void endingWithUnderscoreIsOk() {
    validate("validRegionName_");
  }

  @Test
  public void containingDoubleUnderscoreIsOk() {
    validate("valid__regionName");
  }

  @Test
  public void endingWithDoubleUnderscoreIsOk() {
    validate("validRegionName__");
  }

  @Test
  public void startingWithHyphenIsOk() {
    validate("-validRegionName");
  }

  @Test
  public void startingWithDoubleHyphenIsOk() {
    validate("--validRegionName");
  }

  @Test
  public void containingHyphenIsOk() {
    validate("valid-regionName");
  }

  @Test
  public void containingDoubleHyphenIsOk() {
    validate("valid--regionName");
  }

  @Test
  public void endingWithHyphenIsOk() {
    validate("validRegionName-");
  }

  @Test
  public void endingWithDoubleHyphenIsOk() {
    validate("validRegionName--");
  }

  @Test
  public void startingWithMatchingCharactersAreOk() {
    for (char character = 0; character < MAX_VALUE; character++) {
      String name = character + "ValidRegion";
      Matcher matcher = getNamePattern().matcher(name);
      if (matcher.matches()) {
        validate(name);
      }
    }
  }

  @Test
  public void containingMatchingCharactersAreOk() {
    for (char character = 0; character < MAX_VALUE; character++) {
      String name = "Valid" + character + "Region";
      Matcher matcher = getNamePattern().matcher(name);
      if (matcher.matches()) {
        validate(name);
      }
    }
  }

  @Test
  public void endingMatchingCharactersAreOk() {
    for (char character = 0; character < MAX_VALUE; character++) {
      String name = "ValidRegion" + character;
      Matcher matcher = getNamePattern().matcher(name);
      if (matcher.matches()) {
        validate(name);
      }
    }
  }

  @Test
  public void startingWithNonMatchingCharactersThrow() {
    for (char character = 0; character < MAX_VALUE; character++) {
      String name = character + "InvalidRegion";
      Matcher matcher = getNamePattern().matcher(name);
      if (!matcher.matches()) {
        assertThatThrownBy(() -> validate(name)).isInstanceOf(IllegalArgumentException.class);
      }
    }
  }

  @Test
  public void containingNonMatchingCharactersThrow() {
    for (char character = 0; character < MAX_VALUE; character++) {
      String name = "Invalid" + character + "Region";
      Matcher matcher = getNamePattern().matcher(name);
      if (!matcher.matches()) {
        assertThatThrownBy(() -> validate(name)).isInstanceOf(IllegalArgumentException.class);
      }
    }
  }

  @Test
  public void endingWithNonMatchingCharactersThrow() {
    for (char character = 0; character < MAX_VALUE; character++) {
      String name = "InvalidRegion" + character;
      Matcher matcher = getNamePattern().matcher(name);
      if (!matcher.matches()) {
        assertThatThrownBy(() -> validate(name)).isInstanceOf(IllegalArgumentException.class);
      }
    }
  }
}
