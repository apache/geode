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

import org.apache.geode.pdx.PdxInstance;

public interface JSONToPdxMapper {

  JSONToPdxMapper getParent();

  void setPdxFieldName(String name);

  void addStringField(String fieldName, String value);

  void addByteField(String fieldName, byte value);

  void addShortField(String fieldName, short value);

  void addIntField(String fieldName, int value);

  void addLongField(String fieldName, long value);

  void addBigDecimalField(String fieldName, BigDecimal value);

  void addBigIntegerField(String fieldName, BigInteger value);

  void addBooleanField(String fieldName, boolean value);

  void addFloatField(String fieldName, float value);

  void addDoubleField(String fieldName, double value);

  void addNullField(String fieldName);

  void addListField(String fieldName, PdxListHelper list);

  void endListField(String fieldName);

  void addObjectField(String fieldName, Object member);

  void endObjectField(String fieldName);

  PdxInstance getPdxInstance();

  String getPdxFieldName();

}
