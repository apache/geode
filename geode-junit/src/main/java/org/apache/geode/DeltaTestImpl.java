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
package org.apache.geode;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.TestObjectWithIdentifier;

/**
 * Sample test class which implements Delta.
 *
 * @since GemFire 6.1
 */
public class DeltaTestImpl implements DataSerializable, Delta {

  private static final byte INT_MASK = 0x1;
  private static final byte STR_MASK = 0x2;
  private static final byte DOUBLE_MASK = 0x4;
  private static final byte BYTE_ARR_MASK = 0x8;
  private static final byte TEST_OBJ_MASK = 0x10;
  private static final byte COMPLETE_MASK = 0x1F;

  /*****************************************************************************
   * Below fields are not part of standard Delta implementation but are used for testing purpose.
   */
  public static final String ERRONEOUS_STRING_FOR_FROM_DELTA = "ERRONEOUS_STRING";
  public static final int ERRONEOUS_INT_FOR_TO_DELTA = -101;
  private static long fromDeltaInvokations;
  private static long toDeltaInvokations;
  private static long toDeltaFailure;
  private static long fromDeltaFailure;
  private static long timesConstructed = 0;
  public static boolean NEED_TO_RESET_T0_DELTA = true;
  /** *********************************************************************** */

  // Actual data fields of this instance.
  private int intVar = 0; // 0000 0001
  private String str = ""; // 0000 0010
  private Double doubleVar = new Double(0); // 0000 0100
  private byte[] byteArr = new byte[1]; // 0000 1000
  private TestObjectWithIdentifier testObj = new TestObjectWithIdentifier(); // 0001 0000

  /**
   * Indicates the fields containing delta.
   */
  private byte deltaBits = 0x0;

  private boolean hasDelta = false;

  private static List<Exception> instantiations = new ArrayList<>();

  public static List<Exception> getInstantiations() {
    return instantiations;
  }

  public DeltaTestImpl() {
    timesConstructed++;
    instantiations.add(new Exception("DeltaTestImpl"));
  }

  public DeltaTestImpl(int intVal, String str) {
    this.intVar = intVal;
    this.str = str;
  }

  public DeltaTestImpl(int intVal, String string, Double doubleVal, byte[] bytes,
      TestObjectWithIdentifier testObj) {
    this.intVar = intVal;
    this.str = string;
    this.doubleVar = doubleVal;
    this.byteArr = bytes;
    this.testObj = testObj;
  }

  public static long getTimesConstructed() {
    return timesConstructed;
  }

  public static void setTimesConstructed(long cnt) {
    timesConstructed = cnt;
  }

  public void resetDeltaStatus() {
    this.deltaBits = 0x0;
    this.hasDelta = false;
  }

  public byte[] getByteArr() {
    return byteArr;
  }

  public void setByteArr(byte[] bytes) {
    this.byteArr = bytes;
    this.deltaBits |= BYTE_ARR_MASK;
    this.hasDelta = true;
  }

  public Double getDoubleVar() {
    return doubleVar;
  }

  public void setDoubleVar(Double doubleVar) {
    this.doubleVar = doubleVar;
    this.deltaBits |= DOUBLE_MASK;
    this.hasDelta = true;
  }

  public int getIntVar() {
    return intVar;
  }

  public void setIntVar(int intVar) {
    this.intVar = intVar;
    this.deltaBits |= INT_MASK;
    this.hasDelta = true;
  }

  public String getStr() {
    return str;
  }

  public void setStr(String str) {
    this.str = str;
    this.deltaBits |= STR_MASK;
    this.hasDelta = true;
  }

  public TestObjectWithIdentifier getTestObj() {
    return testObj;
  }

  public void setTestObj(TestObjectWithIdentifier testObj) {
    this.testObj = testObj;
    this.deltaBits |= TEST_OBJ_MASK;
    this.hasDelta = true;
  }

  /*****************************************************************************
   * Below methods are not part of standard Delta implementation but are used for testing purpose.
   */
  public static void resetDeltaInvokationCounters() {
    resetToDeltaCounter();
    resetFromDeltaCounter();
    resetFailureCounter();
  }

  public static void resetToDeltaCounter() {
    toDeltaInvokations = 0;
  }

  public static void resetFromDeltaCounter() {
    fromDeltaInvokations = 0;
  }

  public static void resetFailureCounter() {
    toDeltaFailure = 0;
    fromDeltaFailure = 0;
  }

  public static Boolean deltaFeatureUsed() {
    return (toDeltaInvokations > 0) || (fromDeltaInvokations > 0);
  }

  public static Boolean toDeltaFeatureUsed() {
    return (toDeltaInvokations > 0);
  }

  public static Long getFromDeltaInvokations() {
    return fromDeltaInvokations;
  }

  public static Long getToDeltaInvokations() {
    return toDeltaInvokations;
  }

  public static Boolean fromDeltaFeatureUsed() {
    return (fromDeltaInvokations > 0);
  }

  public static Boolean isFromDeltaFailure() {
    return (fromDeltaFailure > 0);
  }

  public static Boolean isToDeltaFailure() {
    return (toDeltaFailure > 0);
  }

  public static Long getFromDeltaFailures() {
    return fromDeltaFailure;
  }

  public static Long getToDeltaFailures() {
    return toDeltaFailure;
  }

  protected void checkInvalidString(String str) {
    if (ERRONEOUS_STRING_FOR_FROM_DELTA.equals(str)) {
      fromDeltaFailure++;
      throw new InvalidDeltaException("Delta could not be applied. " + this);
    }
  }

  protected void checkInvalidInt(int intVal) {
    if (ERRONEOUS_INT_FOR_TO_DELTA == intVal) {
      toDeltaFailure++;
      throw new InvalidDeltaException("Delta could not be extracted. " + this);
    }
  }

  protected void checkInvalidInt2(int intVal) {}

  /** ********************************************************************** */

  @Override
  public String toString() {
    StringBuffer bytes = new StringBuffer("");
    if (byteArr != null) {
      for (int i = 0; i < byteArr.length; i++) {
        bytes.append(byteArr[i]);
      }
    }
    return "DeltaTestImpl[hasDelta=" + this.hasDelta + ",int=" + this.intVar + ",double="
        + this.doubleVar + ",str=" + this.str + ",bytes={" + bytes.toString() + "},testObj="
        + ((this.testObj != null) ? this.testObj.hashCode() : "") + "]";
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof DeltaTestImpl)) {
      return false;
    }
    DeltaTestImpl delta = (DeltaTestImpl) other;
    if (this.intVar == delta.intVar && this.doubleVar.equals(delta.doubleVar)
        && Arrays.equals(this.byteArr, delta.byteArr) && this.str.equals(delta.str)) {
      return true;
    }
    return false;
  }

  @Override
  public void fromDelta(DataInput in) throws IOException {
    try {
      fromDeltaInvokations++;
      boolean tempHasDelta = false;
      byte tempDeltaBits = this.deltaBits;
      byte[] tempByteArr = this.byteArr;
      int tempIntVar = this.intVar;
      double tempDoubleVar = this.doubleVar;
      String tempStr = this.str;
      TestObjectWithIdentifier tempTestObj = this.testObj;

      tempDeltaBits = DataSerializer.readByte(in);
      if (tempDeltaBits != 0) {
        tempHasDelta = true;
        if ((tempDeltaBits & INT_MASK) == INT_MASK) {
          tempIntVar = DataSerializer.readPrimitiveInt(in);
          checkInvalidInt2(tempIntVar);
        }
        if ((tempDeltaBits & STR_MASK) == STR_MASK) {
          tempStr = DataSerializer.readString(in);
          checkInvalidString(tempStr); // Simulates exception
        }
        if ((tempDeltaBits & DOUBLE_MASK) == DOUBLE_MASK) {
          tempDoubleVar = DataSerializer.readDouble(in);
        }
        if ((tempDeltaBits & BYTE_ARR_MASK) == BYTE_ARR_MASK) {
          tempByteArr = DataSerializer.readByteArray(in);
        }
        if ((tempDeltaBits & TEST_OBJ_MASK) == TEST_OBJ_MASK) {
          tempTestObj = DataSerializer.readObject(in);
        }
        if ((deltaBits | COMPLETE_MASK) != COMPLETE_MASK) {
          throw new IllegalArgumentException("Unknown field code: " + tempDeltaBits);
        }
      }
      if (tempHasDelta) {
        this.intVar = tempIntVar;
        this.str = tempStr;
        this.doubleVar = tempDoubleVar;
        this.byteArr = tempByteArr;
        this.testObj = tempTestObj;
      }
    } catch (IOException e) {
      GemFireCacheImpl.getInstance().getLogger().warning("DeltaTestImpl.fromDelta(): " + e);
      throw e;
    } catch (IllegalArgumentException iae) {
      GemFireCacheImpl.getInstance().getLogger().warning("DeltaTestImpl.fromDelta(): " + iae);
      throw new InvalidDeltaException(iae);
    } catch (ClassNotFoundException cnfe) {
      GemFireCacheImpl.getInstance().getLogger().warning("DeltaTestImpl.fromDelta(): " + cnfe);
      throw new InvalidDeltaException(cnfe);
    }
  }

  @Override
  public boolean hasDelta() {
    return this.hasDelta;
  }

  @Override
  public void toDelta(DataOutput out) throws IOException {
    try {
      toDeltaInvokations++;
      DataSerializer.writeByte(this.deltaBits, out);

      if (deltaBits != 0) {
        if ((deltaBits & INT_MASK) == INT_MASK) {
          DataSerializer.writePrimitiveInt(this.intVar, out);
          checkInvalidInt(this.intVar); // Simulates exception
        }
        if ((deltaBits & STR_MASK) == STR_MASK) {
          DataSerializer.writeString(this.str, out);
        }
        if ((deltaBits & DOUBLE_MASK) == DOUBLE_MASK) {
          DataSerializer.writeDouble(this.doubleVar, out);
        }
        if ((deltaBits & BYTE_ARR_MASK) == BYTE_ARR_MASK) {
          DataSerializer.writeByteArray(this.byteArr, out);
        }
        if ((deltaBits & TEST_OBJ_MASK) == TEST_OBJ_MASK) {
          DataSerializer.writeObject(this.testObj, out);
        }
        if ((deltaBits | COMPLETE_MASK) != COMPLETE_MASK) {
          throw new IllegalArgumentException("Unknown field code: " + deltaBits);
        }
      }
    } catch (IOException ioe) {
      GemFireCacheImpl.getInstance().getLogger().warning("DeltaTestImpl.toDelta(): " + ioe);
      throw ioe;
    } catch (IllegalArgumentException iae) {
      GemFireCacheImpl.getInstance().getLogger().warning("DeltaTestImpl.toDelta(): " + iae);
      throw new InvalidDeltaException(iae);
    } finally {
      if (NEED_TO_RESET_T0_DELTA) {
        // No need to reset if secondary needs to send delta again upon receiving
        // forceReattemptException
        this.deltaBits = 0x0;
        this.hasDelta = false;
      }
    }
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.intVar = DataSerializer.readPrimitiveInt(in);
    this.str = DataSerializer.readString(in);
    this.doubleVar = DataSerializer.readDouble(in);
    this.byteArr = DataSerializer.readByteArray(in);
    this.testObj = DataSerializer.readObject(in);
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writePrimitiveInt(this.intVar, out);
    DataSerializer.writeString(this.str, out);
    DataSerializer.writeDouble(this.doubleVar, out);
    DataSerializer.writeByteArray(this.byteArr, out);
    DataSerializer.writeObject(this.testObj, out);
  }
}
