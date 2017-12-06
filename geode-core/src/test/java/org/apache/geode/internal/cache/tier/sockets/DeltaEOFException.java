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

import org.apache.geode.DataSerializer;
import org.apache.geode.InvalidDeltaException;
import org.apache.geode.internal.cache.GemFireCacheImpl;

/**
 * Faulty delta implementation, raising EndOfFile exception as fromDelta reads more fields then
 * wrote by toDelta
 *
 * @since GemFire 6.1
 */
public class DeltaEOFException extends FaultyDelta {

  public void fromDelta(DataInput in) throws IOException, InvalidDeltaException {
    try {
      byte deltaBits = DataSerializer.readByte(in);
      GemFireCacheImpl.getInstance().getLogger().fine("Applying delta on " + this.toString());
      if (deltaBits != 0) {
        if ((deltaBits & INT_MASK) == INT_MASK) {
          this.intVal = DataSerializer.readPrimitiveInt(in);
          GemFireCacheImpl.getInstance().getLogger()
              .fine(" Applied delta on DeltaImpl's field 'intVal' = " + this.intVal);
        }
        if ((deltaBits & BIG_OBJECT_MASK) == BIG_OBJECT_MASK) {
          this.bigObj = DataSerializer.readByteArray(in);
          GemFireCacheImpl.getInstance().getLogger()
              .fine(" Applied delta on DeltaImpl's field 'bigObj' = {" + this.bigObj[0] + " "
                  + this.bigObj[1] + "}");
        }
        if ((deltaBits | COMPLETE_MASK) != COMPLETE_MASK) {
          GemFireCacheImpl.getInstance().getLogger().fine(" <unknown field code>");
          throw new IllegalArgumentException(
              "DeltaImpl.fromDelta(): Unknown field code, " + deltaBits);
        }
      }
      /*
       * we are reading addition field here. Its is done intentionly to produce faulty fromDelta
       * implementation
       */
      DataSerializer.readByte(in);

      GemFireCacheImpl.getInstance().getLogger().fine(" Reading extra DeltaObj's field 'byte' ");
    } catch (IOException ioe) {
      GemFireCacheImpl.getInstance().getLogger().warning("DeltaObj.fromDelta(): " + ioe);
      throw ioe;
    } catch (IllegalArgumentException iae) {
      GemFireCacheImpl.getInstance().getLogger().warning("DeltaObj.fromDelta(): " + iae);
      throw new InvalidDeltaException(iae);
    }
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
}
