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
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.pdx.FieldType;
import org.apache.geode.pdx.JSONFormatter;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxInstanceFactory;

/**
 * This class is intermediate class to create PdxInstance.
 */
public class PdxInstanceSortedHelper implements JSONToPdxMapper {
  private static final Logger logger = LogService.getLogger();

  JSONToPdxMapper m_parent;
  LinkedList<JSONFieldHolder<?>> fieldList = new LinkedList<>();
  PdxInstance m_pdxInstance;
  String m_PdxName;// when pdx is member, else null if part of lists
  private Set<String> identityFields;

  private InternalCache getCache() {
    return (InternalCache) CacheFactory.getAnyInstance();
  }

  public PdxInstanceSortedHelper(String className, JSONToPdxMapper parent,
      String... identityFields) {
    if (logger.isTraceEnabled()) {
      logger.trace("ClassName {}", className);
    }
    m_PdxName = className;
    m_parent = parent;
    initializeIdentityFields(identityFields);
  }

  public void initializeIdentityFields(String... identityFields) {
    this.identityFields = new HashSet<>();
    for (String identityField : identityFields) {
      this.identityFields.add(identityField);
    }
  }

  @Override
  public JSONToPdxMapper getParent() {
    return m_parent;
  }

  @Override
  public void setPdxFieldName(String name) {
    if (logger.isTraceEnabled()) {
      logger.trace("setPdxClassName : {}", name);
    }
    m_PdxName = name;
  }

  static class JSONFieldHolder<T> implements Comparable<JSONFieldHolder> {
    private final String fieldName;
    private final T value;
    private final FieldType type;

    public JSONFieldHolder(String fn, T v, FieldType ft) {
      fieldName = fn;
      value = v;
      type = ft;
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

  @Override
  public void addStringField(String fieldName, String value) {
    if (logger.isTraceEnabled()) {
      logger.trace("addStringField fieldName: {}; value: {}", fieldName, value);
    }
    fieldList.add(new JSONFieldHolder(fieldName, value, FieldType.STRING));
  }

  @Override
  public void addByteField(String fieldName, byte value) {
    if (logger.isTraceEnabled()) {
      logger.trace("addByteField fieldName: {}; value: {}", fieldName, value);
    }
    fieldList.add(new JSONFieldHolder(fieldName, value, FieldType.BYTE));
  }

  @Override
  public void addShortField(String fieldName, short value) {
    if (logger.isTraceEnabled()) {
      logger.trace("addShortField fieldName: {}; value: {}", fieldName, value);
    }
    fieldList.add(new JSONFieldHolder(fieldName, value, FieldType.SHORT));
  }

  @Override
  public void addIntField(String fieldName, int value) {
    if (logger.isTraceEnabled()) {
      logger.trace("addIntField fieldName: {}; value: {}", fieldName, value);
    }
    fieldList.add(new JSONFieldHolder(fieldName, value, FieldType.INT));
  }

  @Override
  public void addLongField(String fieldName, long value) {
    if (logger.isTraceEnabled()) {
      logger.trace("addLongField fieldName: {}; value: {}", fieldName, value);
    }
    fieldList.add(new JSONFieldHolder(fieldName, value, FieldType.LONG));
  }

  @Override
  public void addBigDecimalField(String fieldName, BigDecimal value) {
    if (logger.isTraceEnabled()) {
      logger.trace("addBigDecimalField fieldName: {}; value: {}", fieldName, value);
    }
    fieldList.add(new JSONFieldHolder(fieldName, value, FieldType.OBJECT));
  }

  @Override
  public void addBigIntegerField(String fieldName, BigInteger value) {
    if (logger.isTraceEnabled()) {
      logger.trace("addBigIntegerField fieldName: {}; value: {}", fieldName, value);
    }
    fieldList.add(new JSONFieldHolder(fieldName, value, FieldType.OBJECT));
  }

  @Override
  public void addBooleanField(String fieldName, boolean value) {
    if (logger.isTraceEnabled()) {
      logger.trace("addBooleanField fieldName: {}; value: {}", fieldName, value);
    }
    fieldList.add(new JSONFieldHolder(fieldName, value, FieldType.BOOLEAN));
  }

  @Override
  public void addFloatField(String fieldName, float value) {
    if (logger.isTraceEnabled()) {
      logger.trace("addFloatField fieldName: {}; value: {}", fieldName, value);
    }
    fieldList.add(new JSONFieldHolder(fieldName, value, FieldType.FLOAT));
  }

  @Override
  public void addDoubleField(String fieldName, double value) {
    if (logger.isTraceEnabled()) {
      logger.trace("addDoubleField fieldName: {}; value: {}", fieldName, value);
    }
    fieldList.add(new JSONFieldHolder(fieldName, value, FieldType.DOUBLE));
  }

  @Override
  public void addNullField(String fieldName) {
    if (logger.isTraceEnabled()) {
      logger.trace("addNullField fieldName: {}; value: NULL", fieldName);
    }
    fieldList.add(new JSONFieldHolder(fieldName, null, FieldType.OBJECT));
  }

  @Override
  public void addListField(String fieldName, PdxListHelper list) {
    if (logger.isTraceEnabled()) {
      logger.trace("addListField fieldName: {}", fieldName);
    }
    // fieldNameVsType.put(fieldName, list.getList());
    fieldList.add(new JSONFieldHolder(fieldName, list.getList(), FieldType.OBJECT));
  }

  @Override
  public void endListField(String fieldName) {
    if (logger.isTraceEnabled()) {
      logger.trace("endListField fieldName: {}", fieldName);
    }
  }

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
      addIdentityField(factory, f.fieldName);
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
        throw new RuntimeException("Unable to convert json field " + key);
    }
  }

  private void addIdentityField(PdxInstanceFactory factory, String fieldName) {
    if (identityFields.contains(fieldName)) {
      factory.markIdentityField(fieldName);
    }
  }

  private PdxInstanceFactory createPdxInstanceFactory() {
    InternalCache cache = getCache();
    return cache.createPdxInstanceFactory(JSONFormatter.JSON_CLASSNAME, false);
  }

  @Override
  public PdxInstance getPdxInstance() {
    return m_pdxInstance;
  }

  @Override
  public String getPdxFieldName() {
    return m_PdxName;
  }
}
