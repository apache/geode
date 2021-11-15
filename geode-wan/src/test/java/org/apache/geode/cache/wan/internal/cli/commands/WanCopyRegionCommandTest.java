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

import static org.apache.geode.cache.wan.internal.cli.commands.WanCopyRegionCommand.WAN_COPY_REGION__BATCHSIZE;
import static org.apache.geode.cache.wan.internal.cli.commands.WanCopyRegionCommand.WAN_COPY_REGION__CANCEL;
import static org.apache.geode.cache.wan.internal.cli.commands.WanCopyRegionCommand.WAN_COPY_REGION__MAXRATE;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.test.junit.rules.GfshParserRule;

public class WanCopyRegionCommandTest {
  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  @Test
  public void gfshParserReturnsNullIfMandatoryOptionsNotSpecified() {
    assertThat(gfsh.parse("wan-copy region")).isNull();
    assertThat(gfsh.parse("wan-copy region --region=myregion")).isNull();
    assertThat(gfsh.parse("wan-copy region --sender-id=ln")).isNull();
    assertThat(gfsh.parse("wan-copy region --batch-size=10")).isNull();
  }

  @Test
  public void verifyDefaultValues() {
    GfshParseResult result = gfsh.parse("wan-copy region --region=myregion --sender-id=ln");
    assertThat(result.getParamValueAsString(WAN_COPY_REGION__MAXRATE)).isEqualTo("0");
    assertThat(result.getParamValueAsString(WAN_COPY_REGION__BATCHSIZE))
        .isEqualTo("1000");
    assertThat(result.getParamValueAsString(WAN_COPY_REGION__CANCEL))
        .isEqualTo("false");
  }
}
