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

package org.apache.geode.cache.lucene.internal.repository.serializer;

import java.io.Serializable;

/**
 * A test type to get mapped to a lucene document
 */
public class Type1 implements Serializable {
  private static final long serialVersionUID = 1L;

  public static final String[] fields = new String[] {"s", "i", "l", "d", "f"};

  String s;
  int i;
  long l;
  double d;
  float f;
  Serializable o = new Serializable() {
    private static final long serialVersionUID = 1L;
  };

  public Type1(String s, int i, long l, double d, float f) {
    this.s = s;
    this.i = i;
    this.l = l;
    this.d = d;
    this.f = f;
  }
}
