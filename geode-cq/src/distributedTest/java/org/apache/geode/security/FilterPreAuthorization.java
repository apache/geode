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

package org.apache.geode.security;

import java.security.Principal;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.Instantiator;
import org.apache.geode.LogWriter;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.operations.OperationContext;
import org.apache.geode.cache.operations.OperationContext.OperationCode;
import org.apache.geode.cache.operations.PutAllOperationContext;
import org.apache.geode.cache.operations.PutOperationContext;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.Version;

/**
 * An authorization implementation for testing that changes a string value in pre-operation phase to
 * add an integer denoting which <code>Principal</code>s would be allowed to get that object.
 *
 * @since GemFire 5.5
 */
public class FilterPreAuthorization implements AccessControl {

  private LogWriter logger;

  static {
    Instantiator.register(new Instantiator(ObjectWithAuthz.class, ObjectWithAuthz.CLASSID) {
      @Override
      public DataSerializable newInstance() {
        return new ObjectWithAuthz();
      }
    }, false);
  }

  public FilterPreAuthorization() {

    this.logger = null;
  }

  public static AccessControl create() {

    return new FilterPreAuthorization();
  }

  public void init(Principal principal, DistributedMember remoteMember, Cache cache)
      throws NotAuthorizedException {

    this.logger = cache.getSecurityLogger();
  }

  public boolean authorizeOperation(String regionName, OperationContext context) {

    assert !context.isPostOperation();
    OperationCode opCode = context.getOperationCode();
    if (opCode.isPut()) {
      PutOperationContext createContext = (PutOperationContext) context;
      // byte[] serializedValue = createContext.getSerializedValue();
      byte[] serializedValue = null;
      Object value = createContext.getValue();
      int valLength;
      byte lastByte;
      if (value == null) {
        // This means serializedValue too is null.
        valLength = 0;
        lastByte = 0;
      } else {
        if (value instanceof byte[]) {
          serializedValue = (byte[]) value;
          valLength = serializedValue.length;
          lastByte = serializedValue[valLength - 1];
        } else {
          ObjectWithAuthz authzObj = new ObjectWithAuthz(value, Integer.valueOf(value.hashCode()));
          createContext.setValue(authzObj, true);
          return true;
        }
      }
      HeapDataOutputStream hos = new HeapDataOutputStream(valLength + 32, Version.CURRENT);
      try {
        InternalDataSerializer.writeUserDataSerializableHeader(ObjectWithAuthz.CLASSID, hos);
        if (serializedValue != null) {
          hos.write(serializedValue);
        }
        // Some value that determines the Principals that can get this object.
        Integer allowedIndex = Integer.valueOf(lastByte);
        DataSerializer.writeObject(allowedIndex, hos);
      } catch (Exception ex) {
        return false;
      }
      createContext.setSerializedValue(hos.toByteArray(), true);
      if (this.logger.fineEnabled())
        this.logger.fine("FilterPreAuthorization: added authorization " + "info for key: "
            + createContext.getKey());
    } else if (opCode.isPutAll()) {
      PutAllOperationContext createContext = (PutAllOperationContext) context;
      Map map = createContext.getMap();
      Collection entries = map.entrySet();
      Iterator iterator = entries.iterator();
      Map.Entry mapEntry = null;
      while (iterator.hasNext()) {
        mapEntry = (Map.Entry) iterator.next();
        String currkey = (String) mapEntry.getKey();
        Object value = mapEntry.getValue();
        Integer authCode;
        if (value != null) {
          String valStr = value.toString();
          authCode = (int) valStr.charAt(valStr.length() - 1);
        } else {
          authCode = 0;
        }
        ObjectWithAuthz authzObj = new ObjectWithAuthz(value, authCode);
        mapEntry.setValue(authzObj);
        if (this.logger.fineEnabled())
          this.logger.fine(
              "FilterPreAuthorization: putAll: added authorization " + "info for key: " + currkey);
      }
      // Now each of the map's values have become ObjectWithAuthz
    }
    return true;
  }

  public void close() {}

}
