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
package org.apache.geode.pdx.internal.json;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.pdx.FieldType;
import org.apache.geode.pdx.JSONFormatter;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxInstanceFactory;
import org.apache.geode.pdx.internal.PdxInstanceFactoryImpl;

/*
 * This class is intermediate class to create PdxInstance.
 */
public class PdxInstanceSortedHelper implements JSONToPdxMapper {
  private static final Logger logger = LogService.getLogger();

  JSONToPdxMapper m_parent;
  LinkedList<JSONFieldHolder<?>> fieldList = new LinkedList<>();
  PdxInstance m_pdxInstance;
  String m_PdxName;// when pdx is member, else null if part of lists

  public PdxInstanceSortedHelper(String className, JSONToPdxMapper parent) {
    GemFireCacheImpl gci = (GemFireCacheImpl) CacheFactory.getAnyInstance();
    if (logger.isTraceEnabled()) {
      logger.trace("ClassName {}", className);
    }
    m_PdxName = className;
    m_parent = parent;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.geode.pdx.internal.json.JSONToPdxMapper#getParent()
   */
  @Override
  public JSONToPdxMapper getParent() {
    return m_parent;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.geode.pdx.internal.json.JSONToPdxMapper#setPdxFieldName(java.lang.String)
   */
  @Override
  public void setPdxFieldName(String name) {
    if (logger.isTraceEnabled()) {
      logger.trace("setPdxClassName : {}", name);
    }
    m_PdxName = name;
  }

  static class JSONFieldHolder<T> implements Comparable<JSONFieldHolder> {
    private String fieldName;
    private T value;
    private FieldType type;

    public JSONFieldHolder(String fn, T v, FieldType ft) {
      this.fieldName = fn;
      this.value = v;
      this.type = ft;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compareTo(JSONFieldHolder other) {
      return fieldName.compareTo(other.fieldName);
    }

    @Override
    public String toString() {
      return "JSONFieldHolder [fieldName=" + fieldName + ", value=" + value + ", type=" + type
          + "]";
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.geode.pdx.internal.json.JSONToPdxMapper#addStringField(java.lang.String,
   * java.lang.String)
   */
  @Override
  public void addStringField(String fieldName, String value) {
    if (logger.isTraceEnabled()) {
      logger.trace("addStringField fieldName: {}; value: {}", fieldName, value);
    }
    fieldList.add(new JSONFieldHolder(fieldName, value, FieldType.STRING));
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.geode.pdx.internal.json.JSONToPdxMapper#addByteField(java.lang.String, byte)
   */
  @Override
  public void addByteField(String fieldName, byte value) {
    if (logger.isTraceEnabled()) {
      logger.trace("addByteField fieldName: {}; value: {}", fieldName, value);
    }
    fieldList.add(new JSONFieldHolder(fieldName, value, FieldType.BYTE));
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.geode.pdx.internal.json.JSONToPdxMapper#addShortField(java.lang.String, short)
   */
  @Override
  public void addShortField(String fieldName, short value) {
    if (logger.isTraceEnabled()) {
      logger.trace("addShortField fieldName: {}; value: {}", fieldName, value);
    }
    fieldList.add(new JSONFieldHolder(fieldName, value, FieldType.SHORT));
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.geode.pdx.internal.json.JSONToPdxMapper#addIntField(java.lang.String, int)
   */
  @Override
  public void addIntField(String fieldName, int value) {
    if (logger.isTraceEnabled()) {
      logger.trace("addIntField fieldName: {}; value: {}", fieldName, value);
    }
    fieldList.add(new JSONFieldHolder(fieldName, value, FieldType.INT));
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.geode.pdx.internal.json.JSONToPdxMapper#addLongField(java.lang.String, long)
   */
  @Override
  public void addLongField(String fieldName, long value) {
    if (logger.isTraceEnabled()) {
      logger.trace("addLongField fieldName: {}; value: {}", fieldName, value);
    }
    fieldList.add(new JSONFieldHolder(fieldName, value, FieldType.LONG));
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.geode.pdx.internal.json.JSONToPdxMapper#addBigDecimalField(java.lang.String,
   * java.math.BigDecimal)
   */
  @Override
  public void addBigDecimalField(String fieldName, BigDecimal value) {
    if (logger.isTraceEnabled()) {
      logger.trace("addBigDecimalField fieldName: {}; value: {}", fieldName, value);
    }
    fieldList.add(new JSONFieldHolder(fieldName, value, FieldType.OBJECT));
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.geode.pdx.internal.json.JSONToPdxMapper#addBigIntegerField(java.lang.String,
   * java.math.BigInteger)
   */
  @Override
  public void addBigIntegerField(String fieldName, BigInteger value) {
    if (logger.isTraceEnabled()) {
      logger.trace("addBigIntegerField fieldName: {}; value: {}", fieldName, value);
    }
    fieldList.add(new JSONFieldHolder(fieldName, value, FieldType.OBJECT));
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.geode.pdx.internal.json.JSONToPdxMapper#addBooleanField(java.lang.String,
   * boolean)
   */
  @Override
  public void addBooleanField(String fieldName, boolean value) {
    if (logger.isTraceEnabled()) {
      logger.trace("addBooleanField fieldName: {}; value: {}", fieldName, value);
    }
    fieldList.add(new JSONFieldHolder(fieldName, value, FieldType.BOOLEAN));
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.geode.pdx.internal.json.JSONToPdxMapper#addFloatField(java.lang.String, float)
   */
  @Override
  public void addFloatField(String fieldName, float value) {
    if (logger.isTraceEnabled()) {
      logger.trace("addFloatField fieldName: {}; value: {}", fieldName, value);
    }
    fieldList.add(new JSONFieldHolder(fieldName, value, FieldType.FLOAT));
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.geode.pdx.internal.json.JSONToPdxMapper#addDoubleField(java.lang.String,
   * double)
   */
  @Override
  public void addDoubleField(String fieldName, double value) {
    if (logger.isTraceEnabled()) {
      logger.trace("addDoubleField fieldName: {}; value: {}", fieldName, value);
    }
    fieldList.add(new JSONFieldHolder(fieldName, value, FieldType.DOUBLE));
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.geode.pdx.internal.json.JSONToPdxMapper#addNullField(java.lang.String)
   */
  @Override
  public void addNullField(String fieldName) {
    if (logger.isTraceEnabled()) {
      logger.trace("addNullField fieldName: {}; value: NULL", fieldName);
    }
    fieldList.add(new JSONFieldHolder(fieldName, null, FieldType.OBJECT));
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.geode.pdx.internal.json.JSONToPdxMapper#addListField(java.lang.String,
   * org.apache.geode.pdx.internal.json.PdxListHelper)
   */
  @Override
  public void addListField(String fieldName, PdxListHelper list) {
    if (logger.isTraceEnabled()) {
      logger.trace("addListField fieldName: {}", fieldName);
    }
    // fieldNameVsType.put(fieldName, list.getList());
    fieldList.add(new JSONFieldHolder(fieldName, list.getList(), FieldType.OBJECT));
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.geode.pdx.internal.json.JSONToPdxMapper#endListField(java.lang.String)
   */
  @Override
  public void endListField(String fieldName) {
    if (logger.isTraceEnabled()) {
      logger.trace("endListField fieldName: {}", fieldName);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.geode.pdx.internal.json.JSONToPdxMapper#addObjectField(java.lang.String,
   * org.apache.geode.pdx.PdxInstance)
   */
  @Override
  public void addObjectField(String fieldName, Object member) {
    if (logger.isTraceEnabled()) {
      logger.trace("addObjectField fieldName: {}", fieldName);
    }
    if (fieldName == null) {
      throw new IllegalStateException("addObjectField:Object should have fieldname");
    }
    fieldList.add(new JSONFieldHolder(fieldName, member, FieldType.OBJECT));
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.geode.pdx.internal.json.JSONToPdxMapper#endObjectField(java.lang.String)
   */
  @Override
  public void endObjectField(String fieldName) {
    if (logger.isTraceEnabled()) {
      logger.trace("endObjectField fieldName: {}", fieldName);
    }
    // m_pdxInstance = m_pdxInstanceFactory.create();
    m_pdxInstance = createPdxInstance();
  }

  private PdxInstance createPdxInstance() {
    Collections.sort(fieldList);
    PdxInstanceFactory factory = createPdxInstanceFactory();
    for (JSONFieldHolder<?> f : fieldList) {
      filldata(factory, f);
    }
    return factory.create();
  }

  private void filldata(PdxInstanceFactory factory, JSONFieldHolder key) {
    switch (key.type) {
      case BOOLEAN:
        factory.writeBoolean(key.fieldName, (boolean) key.value);
        break;
      case BYTE:
        factory.writeByte(key.fieldName, (byte) key.value);
        break;
      case SHORT:
        factory.writeShort(key.fieldName, (short) key.value);
        break;
      case INT:
        factory.writeInt(key.fieldName, (int) key.value);
        break;
      case LONG:
        factory.writeLong(key.fieldName, (long) key.value);
        break;
      case FLOAT:
        factory.writeFloat(key.fieldName, (float) key.value);
        break;
      case DOUBLE:
        factory.writeDouble(key.fieldName, (double) key.value);
        break;
      case STRING:
      case OBJECT:
        factory.writeObject(key.fieldName, key.value);
        break;
      default:
        new RuntimeException("Unable to convert json field " + key);
        break;
    }
  }


  static PdxInstanceFactory createPdxInstanceFactory() {
    GemFireCacheImpl gci = (GemFireCacheImpl) CacheFactory.getAnyInstance();
    return gci.createPdxInstanceFactory(JSONFormatter.JSON_CLASSNAME, false);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.geode.pdx.internal.json.JSONToPdxMapper#getPdxInstance()
   */
  @Override
  public PdxInstance getPdxInstance() {
    return m_pdxInstance;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.geode.pdx.internal.json.JSONToPdxMapper#getPdxFieldName()
   */
  @Override
  public String getPdxFieldName() {
    // return m_fieldName != null ? m_fieldName : "emptyclassname"; //when object is just like { }
    return m_PdxName;
  }
}
