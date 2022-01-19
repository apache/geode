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
package org.apache.geode.internal.cache.tier.sockets;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.Delta;
import org.apache.geode.InvalidDeltaException;
import org.apache.geode.internal.cache.GemFireCacheImpl;

/**
 * Faulty delta implementation, raising ArrayIndexOutOfBound exception as fromDelta reads incorrect
 * sequence then wrote by toDelta
 *
 * @since GemFire 6.1
 */
public class FaultyDelta implements Delta, DataSerializable {

  public static final byte INT_MASK = 0x1;
  public static final byte BIG_OBJECT_MASK = 0x2;
  public static final byte COMPLETE_MASK = 0x3;

  protected int intVal = 0;
  protected boolean hasDelta = false;

  protected byte[] bigObj = new byte[2];

  public static boolean isPatternMatched = false;

  protected byte deltaBits = 0x0;

  @Override
  public void fromDelta(DataInput in) throws IOException, InvalidDeltaException {
    try {
      byte deltaBits = DataSerializer.readByte(in);
      GemFireCacheImpl.getInstance().getLogger().fine("Applying delta on " + this);
      if (deltaBits != 0) {
        // we should read here int, but we are reading byte array. Its is done
        // intentionly to produce faulty fromDelta implementation
        if ((deltaBits & INT_MASK) == INT_MASK) {
          bigObj = DataSerializer.readByteArray(in);
          GemFireCacheImpl.getInstance().getLogger()
              .fine(" Applied delta on DeltaImpl's field 'bigObj' = {" + bigObj[0] + " "
                  + bigObj[1] + "}");
        }
        if ((deltaBits & BIG_OBJECT_MASK) == BIG_OBJECT_MASK) {
          intVal = DataSerializer.readPrimitiveInt(in);
          GemFireCacheImpl.getInstance().getLogger()
              .fine(" Applied delta on DeltaImpl's field 'intVal' = " + intVal);
        }
        if ((deltaBits | COMPLETE_MASK) != COMPLETE_MASK) {
          GemFireCacheImpl.getInstance().getLogger().fine(" <unknown field code>");
          throw new IllegalArgumentException(
              "DeltaImpl.fromDelta(): Unknown field code, " + deltaBits);
        }
      }
    } catch (IOException ioe) {
      GemFireCacheImpl.getInstance().getLogger().warning("DeltaObj.fromDelta(): " + ioe);
      throw ioe;
    } catch (IllegalArgumentException iae) {
      GemFireCacheImpl.getInstance().getLogger().warning("DeltaObj.fromDelta(): " + iae);
      throw new InvalidDeltaException(iae);
    }
  }

  @Override
  public boolean hasDelta() {
    return hasDelta;
  }

  @Override
  public void toDelta(DataOutput out) throws IOException {
    try {
      DataSerializer.writeByte(deltaBits, out);
      GemFireCacheImpl.getInstance().getLogger().fine("Extracting delta from " + this);
      if ((deltaBits & INT_MASK) == INT_MASK) {
        GemFireCacheImpl.getInstance().getLogger()
            .fine(" Extracted delta from DeltaObj's field 'intVal' = " + intVal);
        DataSerializer.writePrimitiveInt(intVal, out);
      }
      if ((deltaBits & BIG_OBJECT_MASK) == BIG_OBJECT_MASK) {
        GemFireCacheImpl.getInstance().getLogger()
            .fine(" Extracted delta from DeltaObj's field 'bigObj' = {" + bigObj[0] + " "
                + bigObj[1] + "}");
        DataSerializer.writeByteArray(bigObj, out);
      }
      if ((deltaBits | COMPLETE_MASK) != COMPLETE_MASK) {
        GemFireCacheImpl.getInstance().getLogger().fine(" <unknown field code>");
        throw new IllegalArgumentException("DeltaImpl.toDelta(): Unknown field code, " + deltaBits);
      }
      DataSerializer.writeByte((byte) 255, out);
      GemFireCacheImpl.getInstance().getLogger()
          .fine(" Writing extra DeltaObj's field 'byte' = " + 255);
      GemFireCacheImpl.getInstance().getLogger().fine("-----------");
      resetDeltaStatus();
    } catch (IOException ioe) {
      GemFireCacheImpl.getInstance().getLogger().warning("DeltaObj.toDelta(): " + ioe);
      throw ioe;
    } catch (IllegalArgumentException iae) {
      GemFireCacheImpl.getInstance().getLogger().warning("DeltaObj.toDelta(): " + iae);
      throw new InvalidDeltaException(iae);
    }

  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    intVal = DataSerializer.readPrimitiveInt(in);
    bigObj = DataSerializer.readByteArray(in);
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writePrimitiveInt(intVal, out);
    DataSerializer.writeByteArray(bigObj, out);
  }

  public byte[] getBigObj() {
    return bigObj;
  }

  public void setBigObj(byte[] bigObj) {
    hasDelta = true;
    deltaBits |= BIG_OBJECT_MASK;
    this.bigObj = bigObj;
  }

  public void resetDeltaStatus() {
    deltaBits = 0x0;
    hasDelta = false;
  }

  public int getIntVal() {
    return intVal;
  }

  public void setIntVal(int intVal) {
    hasDelta = true;
    deltaBits |= INT_MASK;
    this.intVal = intVal;
  }

  public String toString() {
    return "DeltaObj[hasDelta=" + hasDelta + ",intVal=" + intVal + ",bigObj={"
        + bigObj[0] + "," + bigObj[1] + "}]";
  }
}
