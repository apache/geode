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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import org.apache.geode.management.internal.cli.GfshParser;
import org.apache.geode.management.internal.cli.shell.Gfsh;

public class HelpBlock {
  private String data = null;
  private final List<HelpBlock> children = new ArrayList<>();
  // indent level
  private int level = -1;

  public HelpBlock() {}

  public HelpBlock(String data) {
    if (StringUtils.isNotBlank(data)) {
      this.data = data;
      level = 0;
    }
  }

  public String getData() {
    return data;
  }

  public List<HelpBlock> getChildren() {
    return children;
  }

  public int getLevel() {
    return level;
  }

  public void addChild(HelpBlock helpBlock) {
    // before adding another block as the child, increment the indent level
    helpBlock.setLevel(level + 1);
    children.add(helpBlock);
  }

  // recursively set the indent level of the decendents
  public void setLevel(int level) {
    this.level = level;
    for (HelpBlock child : children) {
      child.setLevel(level + 1);
    }
  }

  @Override
  public String toString() {
    // no indentation, no wrapping
    return toString(-1);
  }

  public String toString(int terminalWidth) {
    StringBuffer builder = new StringBuffer();

    if (data != null) {
      builder.append(Gfsh.wrapText(data, level, terminalWidth));
      builder.append(GfshParser.LINE_SEPARATOR);
    }
    for (HelpBlock child : children) {
      builder.append(child.toString(terminalWidth));
    }
    return builder.toString();
  }
}
