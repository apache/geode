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

import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;

public class MyObjectPdxSerializable extends MyObject implements PdxSerializable {
  public MyObjectPdxSerializable() {}

  public MyObjectPdxSerializable(long number, String s) {
    super(number, s);
  }

  @Override
  public void toData(PdxWriter writer) {
    writer.writeLong("f1", f1);
    writer.writeString("f2", f2);
  }

  @Override
  public void fromData(PdxReader reader) {
    f1 = reader.readLong("f1");
    f2 = reader.readString("f2");
  }
}
