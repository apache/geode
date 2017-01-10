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
