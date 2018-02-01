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
package com.examples.snapshot;

import java.io.Serializable;

import org.apache.geode.pdx.PdxInstance;

/**
 * Data class for testing snapshots, cannot be located in org.apache.*.
 *
 */
public class MyObject implements Serializable {
  protected long f1;
  protected String f2;

  public MyObject() {}

  public MyObject(long number, String s) {
    f1 = number;
    f2 = s;
  }

  public long getF1() {
    return f1;
  }

  public String getF2() {
    return f2;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof MyObject) {
      MyObject obj = (MyObject) o;
      return f1 == obj.f1 && f2.equals(obj.f2);

    } else if (o instanceof PdxInstance) {
      PdxInstance pdx = (PdxInstance) o;
      return pdx.getField("f1").equals(f1) && pdx.getField("f2").equals(f2);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return (int) (17 * f1 ^ f2.hashCode());
  }

  public String toString() {
    return f1 + "-" + f2;
  }
}
