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
package org.apache.geode.management.internal.cli.help.format;

import java.util.List;

/**
 *
 */
public class DataNode {
  String data;
  List<DataNode> children;

  public DataNode(String data, List<DataNode> dataNode) {
    this.data = data;
    this.children = dataNode;
  }

  public String getData() {
    return data;
  }

  public List<DataNode> getChildren() {
    return children;
  }

  public boolean addChild(DataNode dataNode) {
    if (this.children != null) {
      this.children.add(dataNode);
      return true;
    } else {
      return false;
    }
  }

}
