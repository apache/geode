/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.security;

import java.security.Principal;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.Instantiator;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.operations.PutAllOperationContext;
import com.gemstone.gemfire.cache.operations.PutOperationContext;
import com.gemstone.gemfire.cache.operations.OperationContext;
import com.gemstone.gemfire.cache.operations.OperationContext.OperationCode;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.HeapDataOutputStream;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.cache.CachedDeserializable;
import com.gemstone.gemfire.internal.cache.CachedDeserializableFactory;
import com.gemstone.gemfire.security.AccessControl;
import com.gemstone.gemfire.security.NotAuthorizedException;

/**
 * An authorization implementation for testing that changes a string value in
 * pre-operation phase to add an integer denoting which <code>Principal</code>s
 * would be allowed to get that object.
 * 
 * @author Sumedh Wale
 * @since 5.5
 */
public class FilterPreAuthorization implements AccessControl {

  private LogWriterI18n logger;

  static {
    Instantiator.register(new Instantiator(ObjectWithAuthz.class,
        ObjectWithAuthz.CLASSID) {
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

  public void init(Principal principal, DistributedMember remoteMember,
      Cache cache) throws NotAuthorizedException {

    this.logger = cache.getSecurityLoggerI18n();
  }

  public boolean authorizeOperation(String regionName, OperationContext context) {

    assert !context.isPostOperation();
    OperationCode opCode = context.getOperationCode();
    if (opCode.isPut()) {
      PutOperationContext createContext = (PutOperationContext)context;
//      byte[] serializedValue = createContext.getSerializedValue();
      byte[] serializedValue = null;
      Object value = createContext.getValue();
      int valLength;
      byte lastByte;
      if (value == null) {
        // This means serializedValue too is null.
        valLength = 0;
        lastByte = 0;
      }
      else {
        if (value instanceof byte[]) {
          serializedValue = (byte[])value;
          valLength = serializedValue.length;
          lastByte = serializedValue[valLength - 1];
        }
        else {
          ObjectWithAuthz authzObj = new ObjectWithAuthz(value, Integer.valueOf(
              value.hashCode()));
          createContext.setValue(authzObj, true);
          return true;
        }
      }
      HeapDataOutputStream hos = new HeapDataOutputStream(valLength + 32, Version.CURRENT);
      try {
        InternalDataSerializer.writeUserDataSerializableHeader(
            ObjectWithAuthz.CLASSID, hos);
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
      this.logger.fine("FilterPreAuthorization: added authorization "
          + "info for key: " + createContext.getKey());
    }
    else if (opCode.isPutAll()) {
      PutAllOperationContext createContext = (PutAllOperationContext)context;
      Map map = createContext.getMap();
      Collection entries = map.entrySet();
      Iterator iterator = entries.iterator();
      Map.Entry mapEntry = null;
      while (iterator.hasNext()) {
        mapEntry = (Map.Entry)iterator.next();
        String currkey = (String)mapEntry.getKey();
        Object value = mapEntry.getValue();
        Integer authCode;
        if (value != null) {
          String valStr = value.toString();
          authCode = (int) valStr.charAt(valStr.length()-1);
        } else {
          authCode = 0;
        }
        ObjectWithAuthz authzObj = new ObjectWithAuthz(value, authCode);
        mapEntry.setValue(authzObj);
        if (this.logger.fineEnabled()) 
        this.logger.fine("FilterPreAuthorization: putAll: added authorization "
            + "info for key: " + currkey);
      }
      // Now each of the map's values have become ObjectWithAuthz
    }
    return true;
  }

  public void close() {
  }

}
