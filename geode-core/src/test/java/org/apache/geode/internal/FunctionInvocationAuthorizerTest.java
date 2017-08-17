package org.apache.geode.internal;


import static org.junit.Assert.assertTrue;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.query.internal.FunctionInvocationAuthorizer;
import org.apache.geode.cache.query.internal.QRegion;
import org.apache.geode.cache.query.internal.index.DummyQRegion;
import org.apache.geode.internal.cache.EntrySnapshot;
import org.apache.geode.internal.cache.LocalRegion;

public class FunctionInvocationAuthorizerTest {

  @Test
  public void toStringOnAnyObject() throws Exception {
    Method stringMethod = Integer.class.getMethod("toString");
    assertTrue(FunctionInvocationAuthorizer.isWhitelisted(stringMethod));
  }

  @Test
  public void equalsOnAnyObject() throws Exception {
    Method equalsMethod = Integer.class.getMethod("equals", Object.class);
    assertTrue(FunctionInvocationAuthorizer.isWhitelisted(equalsMethod));
  }

  @Test
  public void booleanMethodsAreWhiteListed() throws Exception {
    Method booleanValue = Boolean.class.getMethod("booleanValue");
    assertTrue(FunctionInvocationAuthorizer.isWhitelisted(booleanValue));
  }

  @Test
  public void toLowerCaseOnStringObject() throws Exception {
    Method stringMethod = String.class.getMethod("toLowerCase");
    assertTrue(FunctionInvocationAuthorizer.isWhitelisted(stringMethod));
  }

  @Test
  public void toUpperCaseOnStringObject() throws Exception {
    Method stringMethod = String.class.getMethod("toUpperCase");
    assertTrue(FunctionInvocationAuthorizer.isWhitelisted(stringMethod));
  }

  @Test
  public void utilDateAfterMethodIsWhiteListed() throws Exception {
    Method method = Date.class.getMethod("after", Date.class);
    assertTrue(FunctionInvocationAuthorizer.isWhitelisted(method));
  }

  @Test
  public void sqlDateAfterMethodIsWhiteListed() throws Exception {
    Method method = java.sql.Date.class.getMethod("after", Date.class);
    assertTrue(FunctionInvocationAuthorizer.isWhitelisted(method));
  }

  @Test
  public void utilDateBeforeMethodIsWhiteListed() throws Exception {
    Method method = Date.class.getMethod("before", Date.class);
    assertTrue(FunctionInvocationAuthorizer.isWhitelisted(method));
  }

  @Test
  public void sqlDateBeforeMethodIsWhiteListed() throws Exception {
    Method method = java.sql.Date.class.getMethod("before", Date.class);
    assertTrue(FunctionInvocationAuthorizer.isWhitelisted(method));
  }

  @Test
  public void timestampAfterMethodIsWhiteListed() throws Exception {
    Method method = Timestamp.class.getMethod("after", Timestamp.class);
    assertTrue(FunctionInvocationAuthorizer.isWhitelisted(method));
  }

  @Test
  public void sqlTimestampBeforeMethodIsWhiteListed() throws Exception {
    Method method = Timestamp.class.getMethod("before", Timestamp.class);
    assertTrue(FunctionInvocationAuthorizer.isWhitelisted(method));
  }

  @Test
  public void sqlTimestampGetNanosIsWhiteListed() throws Exception {
    Method method = Timestamp.class.getMethod("getNanos");
    assertTrue(FunctionInvocationAuthorizer.isWhitelisted(method));
  }

  @Test
  public void sqlTimestampGetTimeIsWhiteListed() throws Exception {
    Method method = Timestamp.class.getMethod("getTime");
    assertTrue(FunctionInvocationAuthorizer.isWhitelisted(method));
  }


  @Test
  public void getKeyForMapEntryIsWhiteListed() throws Exception {
    Method getKeyMethod = Map.Entry.class.getMethod("getKey");
    assertTrue(FunctionInvocationAuthorizer.isWhitelisted(getKeyMethod));
  }

  @Test
  public void getValueForMapEntryIsWhiteListed() throws Exception {
    Method getValueMethod = Map.Entry.class.getMethod("getValue");
    assertTrue(FunctionInvocationAuthorizer.isWhitelisted(getValueMethod));
  }

  @Test
  public void getKeyForMapEntrySnapShotIsWhiteListed() throws Exception {
    Method getKeyMethod = EntrySnapshot.class.getMethod("getKey");
    assertTrue(FunctionInvocationAuthorizer.isWhitelisted(getKeyMethod));
  }

  @Test
  public void getValueForMapEntrySnapShotIsWhiteListed() throws Exception {
    Method getValueMethod = EntrySnapshot.class.getMethod("getValue");
    assertTrue(FunctionInvocationAuthorizer.isWhitelisted(getValueMethod));
  }

  @Test
  public void getKeyForNonTXEntryIsWhiteListed() throws Exception {
    Method getKeyMethod = LocalRegion.NonTXEntry.class.getMethod("getKey");
    assertTrue(FunctionInvocationAuthorizer.isWhitelisted(getKeyMethod));
  }

  @Test
  public void getValueForNonTXEntryIsWhiteListed() throws Exception {
    Method getValueMethod = LocalRegion.NonTXEntry.class.getMethod("getValue");
    assertTrue(FunctionInvocationAuthorizer.isWhitelisted(getValueMethod));
  }

  @Test
  public void mapMethodsForQRegionAreWhiteListed() throws Exception {
    testMapMethods(QRegion.class);
  }

  @Test
  public void mapMethodsForDummyQRegionAreWhiteListed() throws Exception {
    testMapMethods(DummyQRegion.class);
  }

  @Test
  public void numberMethodsForByteAreWhiteListed() throws Exception {
    testNumberMethods(Byte.class);
  }

  @Test
  public void numberMethodsForDoubleAreWhiteListed() throws Exception {
    testNumberMethods(Double.class);
  }

  @Test
  public void numberMethodsForFloatAreWhiteListed() throws Exception {
    testNumberMethods(Float.class);
  }

  @Test
  public void numberMethodsForIntegerAreWhiteListed() throws Exception {
    testNumberMethods(Integer.class);
  }

  @Test
  public void numberMethodsForShortAreWhiteListed() throws Exception {
    testNumberMethods(Short.class);
  }

  @Test
  public void numberMethodsForBigDecimalAreWhiteListed() throws Exception {
    testNumberMethods(BigDecimal.class);
  }

  @Test
  public void numberMethodsForNumberAreWhiteListed() throws Exception {
    testNumberMethods(BigInteger.class);
  }

  @Test
  public void numberMethodsForAtomicIntegerAreWhiteListed() throws Exception {
    testNumberMethods(AtomicInteger.class);
  }

  @Test
  public void numberMethodsForAtomicLongAreWhiteListed() throws Exception {
    testNumberMethods(AtomicLong.class);
  }

  private void testNumberMethods(Class clazz) throws NoSuchMethodException {
    Method byteValue = clazz.getMethod("byteValue");
    Method doubleValue = clazz.getMethod("doubleValue");
    Method intValue = clazz.getMethod("intValue");
    Method floatValue = clazz.getMethod("longValue");
    Method longValue = clazz.getMethod("floatValue");
    Method shortValue = clazz.getMethod("shortValue");
    assertTrue(FunctionInvocationAuthorizer.isWhitelisted(byteValue));
    assertTrue(FunctionInvocationAuthorizer.isWhitelisted(doubleValue));
    assertTrue(FunctionInvocationAuthorizer.isWhitelisted(intValue));
    assertTrue(FunctionInvocationAuthorizer.isWhitelisted(floatValue));
    assertTrue(FunctionInvocationAuthorizer.isWhitelisted(longValue));
    assertTrue(FunctionInvocationAuthorizer.isWhitelisted(shortValue));
  }

  private void testMapMethods(Class clazz) throws NoSuchMethodException {
    Method entrySet = clazz.getMethod("entrySet");
    Method keySet = clazz.getMethod("keySet");
    Method values = clazz.getMethod("values");
    Method getEntries = clazz.getMethod("getEntries");
    Method getValues = clazz.getMethod("getValues");
    Method containsKey = clazz.getMethod("containsKey", Object.class);
    assertTrue(FunctionInvocationAuthorizer.isWhitelisted(entrySet));
    assertTrue(FunctionInvocationAuthorizer.isWhitelisted(keySet));
    assertTrue(FunctionInvocationAuthorizer.isWhitelisted(values));
    assertTrue(FunctionInvocationAuthorizer.isWhitelisted(getEntries));
    assertTrue(FunctionInvocationAuthorizer.isWhitelisted(getValues));
    assertTrue(FunctionInvocationAuthorizer.isWhitelisted(containsKey));
  }

}
