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
package org.apache.geode.management.internal.cli.converters;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.test.junit.rules.GfshParserRule;
import org.apache.geode.test.junit.rules.GfshParserRule.CommandCandidate;

public class RegionPathConverterJUnitTest {
  @ClassRule
  public static GfshParserRule parser = new GfshParserRule();
  private static RegionPathConverter converter;

  private static final String[] allRegionPaths =
      {SEPARATOR + "region1", SEPARATOR + "region2", SEPARATOR + "rg3"};

  @BeforeClass
  public static void before() {
    // this will let the parser use the spied converter instead of creating its own
    converter = parser.spyConverter(RegionPathConverter.class);
    when(converter.getAllRegionPaths())
        .thenReturn(Arrays.stream(allRegionPaths).collect(Collectors.toSet()));
  }


  @Test
  public void testSupports() throws Exception {
    assertThat(converter.supports(String.class, ConverterHint.REGION_PATH)).isTrue();
  }

  @Test
  public void convert() throws Exception {
    assertThatThrownBy(() -> converter.convertFromText(SEPARATOR, String.class, ""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("invalid region path: " + SEPARATOR);

    assertThat(converter.convertFromText("region", String.class, ""))
        .isEqualTo(SEPARATOR + "region");
    assertThat(converter.convertFromText(SEPARATOR + "region" + SEPARATOR + "t", String.class, ""))
        .isEqualTo(SEPARATOR + "region" + SEPARATOR + "t");
  }

  @Test
  public void complete() throws Exception {
    CommandCandidate candidate = parser.complete("destroy region --name=");
    assertThat(candidate.size()).isEqualTo(allRegionPaths.length);
    assertThat(candidate.getFirstCandidate())
        .isEqualTo("destroy region --name=" + SEPARATOR + "region1");

    candidate = parser.complete("destroy region --name=" + SEPARATOR);
    assertThat(candidate.size()).isEqualTo(allRegionPaths.length);
    assertThat(candidate.getFirstCandidate())
        .isEqualTo("destroy region --name=" + SEPARATOR + "region1");

    candidate = parser.complete("destroy region --name=" + SEPARATOR + "region");
    assertThat(candidate.size()).isEqualTo(2);
    assertThat(candidate.getFirstCandidate())
        .isEqualTo("destroy region --name=" + SEPARATOR + "region1");
  }
}
