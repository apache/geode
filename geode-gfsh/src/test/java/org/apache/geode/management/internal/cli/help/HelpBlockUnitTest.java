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

package org.apache.geode.management.internal.cli.help;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.management.internal.cli.GfshParser;

public class HelpBlockUnitTest {
  private HelpBlock firstBlock, secondBlock, thirdBlock;

  @Before
  public void before() {
    firstBlock = new HelpBlock("First Line");
    secondBlock = new HelpBlock("Second Line");
    thirdBlock = new HelpBlock("Third Line");
    assertThat(firstBlock.getLevel()).isEqualTo(0);
    assertThat(secondBlock.getLevel()).isEqualTo(0);
    assertThat(thirdBlock.getLevel()).isEqualTo(0);
  }

  @Test
  public void testChildLevel() {
    HelpBlock block = new HelpBlock();
    assertThat(block.getLevel()).isEqualTo(-1);

    firstBlock.addChild(secondBlock);
    assertThat(firstBlock.getLevel()).isEqualTo(0);
    assertThat(secondBlock.getLevel()).isEqualTo(1);

    secondBlock.addChild(thirdBlock);
    assertThat(firstBlock.getLevel()).isEqualTo(0);
    assertThat(secondBlock.getLevel()).isEqualTo(1);
    assertThat(thirdBlock.getLevel()).isEqualTo(2);

    assertThat(firstBlock.getChildren()).contains(secondBlock);
    assertThat(firstBlock.getChildren().size()).isEqualTo(1);
    assertThat(secondBlock.getChildren()).contains(thirdBlock);
    assertThat(secondBlock.getChildren().size()).isEqualTo(1);

    // after manually set the level of the first block
    firstBlock.setLevel(10);
    assertThat(firstBlock.getLevel()).isEqualTo(10);
    assertThat(firstBlock.getData()).isEqualTo("First Line");
    assertThat(secondBlock.getLevel()).isEqualTo(11);
    assertThat(secondBlock.getData()).isEqualTo("Second Line");
    assertThat(thirdBlock.getLevel()).isEqualTo(12);
    assertThat(thirdBlock.getData()).isEqualTo("Third Line");
  }

  @Test
  public void testToString() {
    firstBlock.addChild(secondBlock);
    secondBlock.addChild(thirdBlock);

    final String expected = "First Line" + GfshParser.LINE_SEPARATOR
        + "Second Line" + GfshParser.LINE_SEPARATOR
        + "Third Line" + GfshParser.LINE_SEPARATOR;

    String result = firstBlock.toString(-1);
    assertThat(result).isEqualTo(expected);
  }
}
