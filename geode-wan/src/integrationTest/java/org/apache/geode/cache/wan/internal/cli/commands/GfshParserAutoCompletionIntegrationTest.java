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
package org.apache.geode.cache.wan.internal.cli.commands;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.rules.GfshParserRule;
import org.apache.geode.test.junit.rules.GfshParserRule.CommandCandidate;

@Category(GfshTest.class)
public class GfshParserAutoCompletionIntegrationTest {

  @Rule
  public GfshParserRule gfshParserRule = new GfshParserRule();

  @Test
  public void testCompletionOffersMandatoryOptionsInAlphabeticalOrderForWanCopyRegionWithSpace() {
    String buffer = "wan-copy region ";
    CommandCandidate candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(2);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "--region");
    assertThat(candidate.getCandidate(1)).isEqualTo(buffer + "--sender-id");
  }

  @Test
  public void testCompletionOffersTheFirstMandatoryOptionInAlphabeticalOrderForWanCopyRegionWithDash() {
    String buffer = "wan-copy region --";
    CommandCandidate candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(1);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "region");
  }

}
