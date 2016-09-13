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
package org.apache.geode.management.internal.cli.result;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.geode.management.internal.cli.json.GfJsonArray;
import org.apache.geode.management.internal.cli.json.GfJsonException;
import org.apache.geode.management.internal.cli.json.GfJsonObject;

/**
 * 
 * 
 * @since GemFire 7.0
 */
public class ObjectResultData<T extends CliJsonSerializable> extends AbstractResultData {
  public static final String OBJECTS_ACCESSOR = "__objects__";
  
  /*package*/ObjectResultData() {
    super();
  }
  
  /*package*/ObjectResultData(GfJsonObject gfJsonObject) {
    super(gfJsonObject);
  }

  @Override
  public String getType() {
    return TYPE_OBJECT;
  }
  
  public ObjectResultData<T> addObject(T infoBean) {
    try {
      contentObject.accumulateAsJSONObject(OBJECTS_ACCESSOR, infoBean);
    } catch (GfJsonException e) {
      throw new ResultDataException(e.getMessage());
    }
    return this;
  }
  
  public ObjectResultData<T> addCollection(Collection<T> infoBeans) {
    for (T infoBean : infoBeans) {
      try {
        contentObject.accumulateAsJSONObject(OBJECTS_ACCESSOR, infoBean);
      } catch (GfJsonException e) {
        throw new ResultDataException(e.getMessage());
      }
    }
    return this;
  }
  
  public List<CliJsonSerializable> getAllObjects() {
    List<CliJsonSerializable> list = new ArrayList<CliJsonSerializable>();
    try {
      GfJsonArray rootJsonArray = contentObject.getJSONArray(OBJECTS_ACCESSOR);
      int size = rootJsonArray.size();
      
      GfJsonObject jsonObject = null;
      CliJsonSerializable cliJsonSerializable = null;
      for (int i = 0; i < size; i++) {
        jsonObject          = rootJsonArray.getJSONObject(i);
        cliJsonSerializable = CliJsonSerializableFactory.getCliJsonSerializable(jsonObject.getInt(CliJsonSerializable.JSID));
        cliJsonSerializable.fromJson(jsonObject);
        list.add(cliJsonSerializable);
      }
    } catch (GfJsonException e) {
      throw new ResultDataException(e.getMessage());
    }
    return list;
  }
}
