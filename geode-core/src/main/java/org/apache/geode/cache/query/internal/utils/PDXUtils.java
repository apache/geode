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
package org.apache.geode.cache.query.internal.utils;

import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.query.internal.StructImpl;
import org.apache.geode.internal.cache.CachedDeserializable;
import org.apache.geode.internal.cache.VMCachedDeserializable;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.internal.PdxString;

public class PDXUtils {

  public static Object convertPDX(Object obj, boolean isStruct, boolean getDomainObjectForPdx,
      boolean getDeserializedObject, boolean localResults, boolean[] objectChangedMarker,
      boolean isDistinct) {
    objectChangedMarker[0] = false;
    if (isStruct) {
      StructImpl simpl = (StructImpl) obj;
      if (getDomainObjectForPdx) {
        try {
          if (simpl.isHasPdx()) {
            obj = simpl.getPdxFieldValues();
            objectChangedMarker[0] = true;
          } else {
            obj = simpl.getFieldValues();
          }
        } catch (Exception ex) {
          throw new CacheException(
              "Unable to retrieve domain object from PdxInstance while building the ResultSet. "
                  + ex.getMessage()) {};
        }
      } else {
        Object[] values = simpl.getFieldValues();
        if (getDeserializedObject) {
          for (int i = 0; i < values.length; i++) {
            if (values[i] instanceof VMCachedDeserializable) {
              values[i] = ((VMCachedDeserializable) values[i]).getDeserializedForReading();
            }
          }
        }
        /* This is to convert PdxString to String */
        if (simpl.isHasPdx() && isDistinct && localResults) {
          for (int i = 0; i < values.length; i++) {
            if (values[i] instanceof PdxString) {
              values[i] = ((PdxString) values[i]).toString();
            }
          }
        }
        obj = values;
      }
    } else {
      if (getDomainObjectForPdx) {
        if (obj instanceof PdxInstance) {
          try {
            obj = ((PdxInstance) obj).getObject();
            objectChangedMarker[0] = true;
          } catch (Exception ex) {
            throw new CacheException(
                "Unable to retrieve domain object from PdxInstance while building the ResultSet. "
                    + ex.getMessage()) {};
          }
        } else if (obj instanceof PdxString) {
          obj = ((PdxString) obj).toString();
        }
      } else if (isDistinct && localResults && obj instanceof PdxString) {
        /* This is to convert PdxString to String */
        obj = ((PdxString) obj).toString();
      }

      if (getDeserializedObject && obj instanceof CachedDeserializable) {
        obj = ((CachedDeserializable) obj).getDeserializedForReading();
        objectChangedMarker[0] = true;
      }

    }
    return obj;
  }
}
