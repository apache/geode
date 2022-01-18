/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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
package org.apache.geode.cache.lucene.test;

import java.io.Serializable;

public class Page implements Serializable {
  private final int id;
  private final String title;
  private final String content;

  public Page(int id) {
    this.id = id;
    if (id % 2 == 0) {
      title = "manager";
    } else {
      title = "developer";
    }
    content = "Hello world no " + id;
  }

  @Override
  public String toString() {
    return "Page[id=" + id + ",title=" + title + ",content=" + content + "]";
  }
}
