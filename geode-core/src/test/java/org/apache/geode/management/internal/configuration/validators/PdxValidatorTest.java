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
package org.apache.geode.management.internal.configuration.validators;

import static java.util.Collections.emptyList;
import static org.apache.geode.management.internal.CacheElementOperation.CREATE;
import static org.apache.geode.management.internal.CacheElementOperation.DELETE;
import static org.apache.geode.management.internal.CacheElementOperation.UPDATE;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.stream.Stream;

import org.junit.Test;

import org.apache.geode.management.configuration.AutoSerializer;
import org.apache.geode.management.configuration.Pdx;

public class PdxValidatorTest {
  @Test
  public void validateForAnyOperation_acceptsDefaultPdx() {
    PdxValidator validator = new PdxValidator();

    Pdx pdx = new Pdx();

    Stream.of(CREATE, UPDATE, DELETE)
        .forEach(operation -> assertThatCode(() -> validator.validate(operation, pdx))
            .as(operation.name())
            .doesNotThrowAnyException());
  }

  @Test
  public void validateForCreate_throwsIfAutoSerializerHasNoPatterns() {
    PdxValidator validator = new PdxValidator();
    Pdx pdx = new Pdx();

    List<String> noPatterns = emptyList();
    pdx.setAutoSerializer(new AutoSerializer(false, noPatterns));

    assertThatThrownBy(() -> validator.validate(CREATE, pdx))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("at least one pattern");
  }

  @Test
  public void validateForCreate_throwsIfAutoSerializerHasNullPatterns() {
    PdxValidator validator = new PdxValidator();
    Pdx pdx = new Pdx();

    pdx.setAutoSerializer(new AutoSerializer(false, null));

    assertThatThrownBy(() -> validator.validate(CREATE, pdx))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("at least one pattern");
  }

  @Test
  public void validateForDelete_acceptsAutoSerializerWithNoPatterns() {
    PdxValidator validator = new PdxValidator();
    Pdx pdx = new Pdx();

    List<String> noPatterns = emptyList();
    pdx.setAutoSerializer(new AutoSerializer(false, noPatterns));

    assertThatCode(() -> validator.validate(DELETE, pdx))
        .doesNotThrowAnyException();
  }

  @Test
  public void validateForDelete_acceptsAutoSerializerWithNullPatterns() {
    PdxValidator validator = new PdxValidator();
    Pdx pdx = new Pdx();

    pdx.setAutoSerializer(new AutoSerializer(false, null));

    assertThatCode(() -> validator.validate(DELETE, pdx))
        .doesNotThrowAnyException();
  }

  @Test
  public void validateForUpdate_acceptsAutoSerializerWithNullPatterns() {
    PdxValidator validator = new PdxValidator();
    Pdx pdx = new Pdx();

    pdx.setAutoSerializer(new AutoSerializer(false, null));

    assertThatCode(() -> validator.validate(UPDATE, pdx))
        .doesNotThrowAnyException();
  }

  @Test
  public void validateForUpdate_throwsIfAutoSerializerHasNoPatterns() {
    PdxValidator validator = new PdxValidator();
    Pdx pdx = new Pdx();

    List<String> noPatterns = emptyList();
    pdx.setAutoSerializer(new AutoSerializer(false, noPatterns));

    assertThatThrownBy(() -> validator.validate(UPDATE, pdx))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("at least one pattern");
  }

}
