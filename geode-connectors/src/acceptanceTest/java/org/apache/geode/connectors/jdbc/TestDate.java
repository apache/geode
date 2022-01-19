/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.connectors.jdbc;

import java.util.Date;

import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;

public class TestDate implements PdxSerializable {
  public static final String DATE_FIELD_NAME = "mydate";
  private String id;
  private Date myDate;

  public TestDate() {
    // nothing
  }

  public String getId() {
    return id;
  }

  public Date getMyDate() {
    return myDate;
  }

  @Override
  public void toData(PdxWriter writer) {
    writer.writeString("id", id);
    writer.writeDate(DATE_FIELD_NAME, myDate);
  }

  @Override
  public void fromData(PdxReader reader) {
    id = reader.readString("id");
    myDate = reader.readDate(DATE_FIELD_NAME);
  }
}
