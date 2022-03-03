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
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.pdx.JSONFormatter;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.internal.PdxInstanceFactoryImpl;

/**
 * This class is intermediate class to create PdxInstance.
 */
public class PdxInstanceHelper implements JSONToPdxMapper {
  private static final Logger logger = LogService.getLogger();

  JSONToPdxMapper m_parent;
  PdxInstanceFactoryImpl m_pdxInstanceFactory;
  PdxInstance m_pdxInstance;
  String m_PdxName;// when pdx is member, else null if part of lists
  private Set<String> identityFields;

  private InternalCache getCache() {
    return (InternalCache) CacheFactory.getAnyInstance();
  }

  public PdxInstanceHelper(String className, JSONToPdxMapper parent, String... identityFields) {
    InternalCache cache = getCache();
    if (logger.isTraceEnabled()) {
      logger.trace("ClassName {}", className);
    }
    m_PdxName = className;
    m_parent = parent;
    m_pdxInstanceFactory = (PdxInstanceFactoryImpl) cache
        .createPdxInstanceFactory(JSONFormatter.JSON_CLASSNAME, false);
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

  @Override
  public void addStringField(String fieldName, String value) {
    if (logger.isTraceEnabled()) {
      logger.trace("addStringField fieldName: {}; value: {}", fieldName, value);
    }
    m_pdxInstanceFactory.writeObject(fieldName, value);
    addIdentityField(fieldName);
  }

  @Override
  public void addByteField(String fieldName, byte value) {
    if (logger.isTraceEnabled()) {
      logger.trace("addByteField fieldName: {}; value: {}", fieldName, value);
    }
    m_pdxInstanceFactory.writeByte(fieldName, value);
    addIdentityField(fieldName);
  }

  @Override
  public void addShortField(String fieldName, short value) {
    if (logger.isTraceEnabled()) {
      logger.trace("addShortField fieldName: {}; value: {}", fieldName, value);
    }
    m_pdxInstanceFactory.writeShort(fieldName, value);
    addIdentityField(fieldName);
  }

  @Override
  public void addIntField(String fieldName, int value) {
    if (logger.isTraceEnabled()) {
      logger.trace("addIntField fieldName: {}; value: {}", fieldName, value);
    }
    m_pdxInstanceFactory.writeInt(fieldName, value);
    addIdentityField(fieldName);
  }

  @Override
  public void addLongField(String fieldName, long value) {
    if (logger.isTraceEnabled()) {
      logger.trace("addLongField fieldName: {}; value: {}", fieldName, value);
    }
    m_pdxInstanceFactory.writeLong(fieldName, value);
    addIdentityField(fieldName);
  }

  @Override
  public void addBigDecimalField(String fieldName, BigDecimal value) {
    if (logger.isTraceEnabled()) {
      logger.trace("addBigDecimalField fieldName: {}; value: {}", fieldName, value);
    }
    m_pdxInstanceFactory.writeObject(fieldName, value);
    addIdentityField(fieldName);
  }

  @Override
  public void addBigIntegerField(String fieldName, BigInteger value) {
    if (logger.isTraceEnabled()) {
      logger.trace("addBigIntegerField fieldName: {}; value: {}", fieldName, value);
    }
    m_pdxInstanceFactory.writeObject(fieldName, value);
    addIdentityField(fieldName);
  }

  @Override
  public void addBooleanField(String fieldName, boolean value) {
    if (logger.isTraceEnabled()) {
      logger.trace("addBooleanField fieldName: {}; value: {}", fieldName, value);
    }
    m_pdxInstanceFactory.writeBoolean(fieldName, value);
    addIdentityField(fieldName);
  }

  @Override
  public void addFloatField(String fieldName, float value) {
    if (logger.isTraceEnabled()) {
      logger.trace("addFloatField fieldName: {}; value: {}", fieldName, value);
    }
    m_pdxInstanceFactory.writeFloat(fieldName, value);
    addIdentityField(fieldName);
  }

  @Override
  public void addDoubleField(String fieldName, double value) {
    if (logger.isTraceEnabled()) {
      logger.trace("addDoubleField fieldName: {}; value: {}", fieldName, value);
    }
    m_pdxInstanceFactory.writeDouble(fieldName, value);
    addIdentityField(fieldName);
  }

  @Override
  public void addNullField(String fieldName) {
    if (logger.isTraceEnabled()) {
      logger.trace("addNullField fieldName: {}; value: NULL", fieldName);
    }
    m_pdxInstanceFactory.writeObject(fieldName, null);
    addIdentityField(fieldName);
  }

  @Override
  public void addListField(String fieldName, PdxListHelper list) {
    if (logger.isTraceEnabled()) {
      logger.trace("addListField fieldName: {}", fieldName);
    }
    m_pdxInstanceFactory.writeObject(fieldName, list.getList());
    addIdentityField(fieldName);
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
    m_pdxInstanceFactory.writeObject(fieldName, member);
  }

  @Override
  public void endObjectField(String fieldName) {
    if (logger.isTraceEnabled()) {
      logger.trace("endObjectField fieldName: {}", fieldName);
    }
    m_pdxInstance = m_pdxInstanceFactory.create();
  }

  private void addIdentityField(String fieldName) {
    if (identityFields.contains(fieldName)) {
      m_pdxInstanceFactory.markIdentityField(fieldName);
    }
  }

  @Override
  public PdxInstance getPdxInstance() {
    return m_pdxInstance;
  }

  @Override
  public String getPdxFieldName() {
    // return m_fieldName != null ? m_fieldName : "emptyclassname"; //when object is just like { }
    return m_PdxName;
  }
}
