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

  public void fromDelta(DataInput in) throws IOException, InvalidDeltaException {
    try {
      byte deltaBits = DataSerializer.readByte(in);
      GemFireCacheImpl.getInstance().getLogger().fine("Applying delta on " + this.toString());
      if (deltaBits != 0) {
        // we should read here int, but we are reading byte array. Its is done
        // intentionly to produce faulty fromDelta implementation
        if ((deltaBits & INT_MASK) == INT_MASK) {
          this.bigObj = DataSerializer.readByteArray(in);
          GemFireCacheImpl.getInstance().getLogger()
              .fine(" Applied delta on DeltaImpl's field 'bigObj' = {" + this.bigObj[0] + " "
                  + this.bigObj[1] + "}");
        }
        if ((deltaBits & BIG_OBJECT_MASK) == BIG_OBJECT_MASK) {
          this.intVal = DataSerializer.readPrimitiveInt(in);
          GemFireCacheImpl.getInstance().getLogger()
              .fine(" Applied delta on DeltaImpl's field 'intVal' = " + this.intVal);
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

  public boolean hasDelta() {
    return this.hasDelta;
  }

  public void toDelta(DataOutput out) throws IOException {
    try {
      DataSerializer.writeByte(this.deltaBits, out);
      GemFireCacheImpl.getInstance().getLogger().fine("Extracting delta from " + this.toString());
      if ((deltaBits & INT_MASK) == INT_MASK) {
        GemFireCacheImpl.getInstance().getLogger()
            .fine(" Extracted delta from DeltaObj's field 'intVal' = " + this.intVal);
        DataSerializer.writePrimitiveInt(this.intVal, out);
      }
      if ((deltaBits & BIG_OBJECT_MASK) == BIG_OBJECT_MASK) {
        GemFireCacheImpl.getInstance().getLogger()
            .fine(" Extracted delta from DeltaObj's field 'bigObj' = {" + this.bigObj[0] + " "
                + this.bigObj[1] + "}");
        DataSerializer.writeByteArray(this.bigObj, out);
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

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.intVal = DataSerializer.readPrimitiveInt(in);
    this.bigObj = DataSerializer.readByteArray(in);
  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writePrimitiveInt(this.intVal, out);
    DataSerializer.writeByteArray(this.bigObj, out);
  }

  public byte[] getBigObj() {
    return bigObj;
  }

  public void setBigObj(byte[] bigObj) {
    this.hasDelta = true;
    this.deltaBits |= BIG_OBJECT_MASK;
    this.bigObj = bigObj;
  }

  public void resetDeltaStatus() {
    this.deltaBits = 0x0;
    this.hasDelta = false;
  }

  public int getIntVal() {
    return intVal;
  }

  public void setIntVal(int intVal) {
    this.hasDelta = true;
    this.deltaBits |= INT_MASK;
    this.intVal = intVal;
  }

  public String toString() {
    return "DeltaObj[hasDelta=" + this.hasDelta + ",intVal=" + this.intVal + ",bigObj={"
        + this.bigObj[0] + "," + this.bigObj[1] + "}]";
  }
}
