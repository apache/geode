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
package org.apache.geode.cache.query.data;

import java.io.Serializable;

public class Keywords implements Serializable {

  // "select", "distinct", "from", "where", "true", "false","undefined",
  // "element", "not", "and", "or"};
  // }
  public boolean select = true;
  public boolean distinct = true;
  public boolean from = true;
  public boolean where = true;
  public boolean undefined = true;
  public boolean element = true;
  public boolean not = true;
  public boolean and = true;
  public boolean or = true;
  public boolean type = true;

  public boolean SELECT() {
    return true;
  }

  public boolean DISTINCT() {
    return true;
  }

  public boolean FROM() {
    return true;
  }

  public boolean WHERE() {
    return true;
  }

  public boolean UNDEFINED() {
    return true;
  }

  public boolean ELEMENT() {
    return true;
  }

  public boolean TRUE() {
    return true;
  }

  public boolean FALSE() {
    return true;
  }

  public boolean NOT() {
    return true;
  }

  public boolean AND() {
    return true;
  }

  public boolean OR() {
    return true;
  }

  public boolean TYPE() {
    return true;
  }
}
