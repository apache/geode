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
package org.apache.geode.cache.lucene.test;

import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;

public class TestPdxObject extends TestObject implements PdxSerializable {

  // Needed for serialization
  public TestPdxObject() {}

  public TestPdxObject(final String field1, final String field2) {
    super(field1, field2);
  }

  @Override
  public void toData(PdxWriter out) {
    out.writeString("field1", getField1());
    out.writeString("field2", getField1());
  }

  @Override
  public void fromData(PdxReader in) {
    setField1(in.readString("field1"));
    setField2(in.readString("field2"));
  }

}
