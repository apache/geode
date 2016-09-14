/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.management.internal.cli.help.format;

import org.apache.geode.management.internal.cli.GfshParser;
import org.apache.geode.management.internal.cli.shell.Gfsh;

/**
 * 
 * @since GemFire 7.0
 */
public class NewHelp {
  DataNode root;

  public NewHelp(DataNode root) {
    this.root = root;
  }
  
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    append(builder, root, 0);
    return builder.toString();
  }

  private void append(StringBuilder builder, DataNode dataNode, int level) {
    if (dataNode != null && builder != null) {
      String data = dataNode.getData();
      if (data != null && !data.equals("")) {
        builder.append(Gfsh.wrapText(data, level-1));
        builder.append(GfshParser.LINE_SEPARATOR);
      }
      if (dataNode.getChildren() != null && dataNode.getChildren().size() > 0) {
        for (DataNode child : dataNode.getChildren()) {
          append(builder, child, level+1);
        }
      }
    }
  }
}
