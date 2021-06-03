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
import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.logging.internal.log4j.api.LogService;

/*
 * This class is to convert JSON array into List.
 */
public class PdxListHelper {
  private static final Logger logger = LogService.getLogger();

  String m_name;
  PdxListHelper m_parent;
  List list = new LinkedList();

  private InternalCache getCache() {
    return (InternalCache) CacheFactory.getAnyInstance();
  }

  public PdxListHelper(PdxListHelper parent, String name) {
    InternalCache cache = getCache();
    m_name = name;
    if (logger.isTraceEnabled()) {
      logger.trace("PdxListHelper name: {}", name);
    }
    m_parent = parent;
  }

  public PdxListHelper getParent() {
    return m_parent;
  }

  public void setListName(String fieldName) {
    if (logger.isTraceEnabled()) {
      logger.trace("setListName fieldName: {}", fieldName);
    }
    m_name = fieldName;
  }

  public void addStringField(String fieldValue) {
    if (logger.isTraceEnabled()) {
      logger.trace("addStringField fieldValue: {}", fieldValue);
    }
    list.add(fieldValue);
  }

  public void addByteField(byte fieldValue) {
    if (logger.isTraceEnabled()) {
      logger.trace("addByteField fieldValue: {}", fieldValue);
    }
    list.add(fieldValue);
  }

  public void addShortField(short fieldValue) {
    if (logger.isTraceEnabled()) {
      logger.trace("addShortField fieldValue: {}", fieldValue);
    }
    list.add(fieldValue);
  }

  public void addIntField(int fieldValue) {
    if (logger.isTraceEnabled()) {
      logger.trace("addIntField fieldValue: {}", fieldValue);
    }
    list.add(fieldValue);
  }

  public void addLongField(long fieldValue) {
    if (logger.isTraceEnabled()) {
      logger.trace("addLongField fieldValue: {}", fieldValue);
    }
    list.add(fieldValue);
  }

  public void addBigIntegerField(BigInteger fieldValue) {
    if (logger.isTraceEnabled()) {
      logger.trace("addBigIntegerField fieldValue: {}", fieldValue);
    }
    list.add(fieldValue);
  }

  public void addBooleanField(boolean fieldValue) {
    if (logger.isTraceEnabled()) {
      logger.trace("addBooleanField fieldValue: {}", fieldValue);
    }
    list.add(fieldValue);
  }

  public void addFloatField(float fieldValue) {
    if (logger.isTraceEnabled()) {
      logger.trace("addFloatField fieldValue: {}", fieldValue);
    }
    list.add(fieldValue);
  }

  public void addDoubleField(double fieldValue) {
    if (logger.isTraceEnabled()) {
      logger.trace("addDoubleField fieldValue: {}", fieldValue);
    }
    list.add(fieldValue);
  }

  public void addBigDecimalField(BigDecimal fieldValue) {
    if (logger.isTraceEnabled()) {
      logger.trace("addBigDecimalField fieldValue: {}", fieldValue);
    }
    list.add(fieldValue);
  }

  public void addNullField(Object fieldValue) {
    if (logger.isTraceEnabled()) {
      logger.trace("addNULLField fieldValue: {}", fieldValue);
    }
    list.add(fieldValue);
  }

  public PdxListHelper addListField() {
    if (logger.isTraceEnabled()) {
      logger.trace("addListField");
    }
    PdxListHelper tmp = new PdxListHelper(this, "no-name");
    list.add(tmp.getList());
    return tmp;
  }

  public PdxListHelper endListField() {
    if (logger.isTraceEnabled()) {
      logger.trace("endListField");
    }
    return m_parent;
  }

  public void addObjectField(String fieldName, JSONToPdxMapper dpi) {
    if (fieldName != null) {
      throw new IllegalStateException("addObjectField:list should have object no fieldname");
    }
    if (logger.isTraceEnabled()) {
      logger.trace("addObjectField fieldName: {}", fieldName);
    }
    list.add(dpi.getPdxInstance());
  }

  public void endObjectField(String fieldName) {
    if (logger.isTraceEnabled()) {
      logger.trace("endObjectField fieldName: {}", fieldName);
    }
  }

  public List getList() {
    return list;
  }
}
