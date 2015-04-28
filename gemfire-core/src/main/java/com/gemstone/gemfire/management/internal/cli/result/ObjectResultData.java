/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.result;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.gemstone.gemfire.management.internal.cli.json.GfJsonArray;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonException;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonObject;

/**
 * 
 * @author Abhishek Chaudhari
 * 
 * @since 7.0
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
