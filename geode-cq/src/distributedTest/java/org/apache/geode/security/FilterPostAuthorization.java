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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.Instantiator;
import org.apache.geode.LogWriter;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.operations.GetOperationContext;
import org.apache.geode.cache.operations.OperationContext;
import org.apache.geode.cache.operations.OperationContext.OperationCode;
import org.apache.geode.cache.operations.PutOperationContext;
import org.apache.geode.cache.operations.QueryOperationContext;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.internal.CqEntry;
import org.apache.geode.cache.query.internal.ResultsCollectionWrapper;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.Version;

/**
 * An authorization implementation for testing that checks for authorization information in
 * post-operation filtering, removes that field and allows the operation only if the authorization
 * field in {@link ObjectWithAuthz} object allows the current principal.
 *
 * @since GemFire 5.5
 */
public class FilterPostAuthorization implements AccessControl {

  private String principalName;

  private LogWriter logger;

  static {
    Instantiator.register(new Instantiator(ObjectWithAuthz.class, ObjectWithAuthz.CLASSID) {
      @Override
      public DataSerializable newInstance() {
        return new ObjectWithAuthz();
      }
    }, false);
  }

  public FilterPostAuthorization() {

    this.principalName = null;
    this.logger = null;
  }

  public static AccessControl create() {

    return new FilterPostAuthorization();
  }

  public void init(Principal principal, DistributedMember remoteMember, Cache cache)
      throws NotAuthorizedException {

    this.principalName = (principal == null ? "" : principal.getName());
    this.logger = cache.getSecurityLogger();
  }

  private byte[] checkObjectAuth(byte[] serializedObj, boolean isObject) {

    if (!isObject) {
      return null;
    }
    ByteArrayInputStream bis = new ByteArrayInputStream(serializedObj);
    DataInputStream dis = new DataInputStream(bis);
    Object obj;
    try {
      obj = DataSerializer.readObject(dis);
      if (this.logger.finerEnabled()) {
        this.logger.finer("FilterPostAuthorization: successfully read object "
            + "from serialized object: " + obj);
      }
    } catch (Exception ex) {
      this.logger.severe(
          "FilterPostAuthorization: An exception was thrown while trying to de-serialize.",
          ex);
      return null;
    }
    obj = checkObjectAuth(obj);
    if (obj != null) {
      HeapDataOutputStream hos =
          new HeapDataOutputStream(serializedObj.length + 32, Version.CURRENT);
      try {
        DataSerializer.writeObject(obj, hos);
        return hos.toByteArray();
      } catch (Exception ex) {
        this.logger.severe(
            "FilterPostAuthorization: An exception was thrown while trying to serialize.",
            ex);
      }
    }
    return null;
  }

  private Object checkObjectAuth(Object value) {
    Object obj = value;
    if (value instanceof CqEntry) {
      obj = ((CqEntry) value).getValue();
    }

    if (obj instanceof ObjectWithAuthz) {
      int lastChar = this.principalName.charAt(this.principalName.length() - 1) - '0';
      lastChar %= 10;
      ObjectWithAuthz authzObj = (ObjectWithAuthz) obj;
      int authzIndex = ((Integer) authzObj.getAuthz()).intValue() - '0';
      authzIndex %= 10;
      if ((lastChar == 0) || (authzIndex % lastChar != 0)) {
        this.logger.warning(
            String.format(
                "FilterPostAuthorization: The user [%s] is not authorized for the object %s.",
                new Object[] {this.principalName, authzObj.getVal()}));
        return null;
      } else {
        if (this.logger.fineEnabled()) {
          this.logger.fine("FilterPostAuthorization: user [" + this.principalName
              + "] authorized for object: " + authzObj.getVal());
        }
        if (value instanceof CqEntry) {
          return new CqEntry(((CqEntry) value).getKey(), authzObj.getVal());
        } else {
          return authzObj.getVal();
        }
      }
    }
    this.logger.warning(
        String.format("FilterPostAuthorization: The object of type %s, is not an instance of %s.",
            new Object[] {obj.getClass(), ObjectWithAuthz.class}));
    return null;
  }

  public boolean authorizeOperation(String regionName, OperationContext context) {
    assert context.isPostOperation();
    OperationCode opCode = context.getOperationCode();
    if (opCode.isGet()) {
      GetOperationContext getContext = (GetOperationContext) context;
      Object value = getContext.getObject();
      boolean isObject = getContext.isObject();
      if (value != null) {
        if ((value = checkObjectAuth(value)) != null) {
          getContext.setObject(value, isObject);
          return true;
        }
      } else {
        byte[] serializedValue = getContext.getSerializedValue();
        if ((serializedValue = checkObjectAuth(serializedValue, isObject)) != null) {
          getContext.setSerializedValue(serializedValue, isObject);
          return true;
        }
      }
    } else if (opCode.isPut()) {
      PutOperationContext putContext = (PutOperationContext) context;
      byte[] serializedValue = putContext.getSerializedValue();
      boolean isObject = putContext.isObject();
      if ((serializedValue = checkObjectAuth(serializedValue, isObject)) != null) {
        putContext.setSerializedValue(serializedValue, isObject);
        return true;
      }
    } else if (opCode.equals(OperationCode.PUTALL)) {
      // no need for now
    } else if (opCode.isQuery() || opCode.isExecuteCQ()) {
      QueryOperationContext queryContext = (QueryOperationContext) context;
      Object value = queryContext.getQueryResult();
      if (value instanceof SelectResults) {
        SelectResults results = (SelectResults) value;
        List newResults = new ArrayList();
        Iterator resultIter = results.iterator();
        while (resultIter.hasNext()) {
          Object obj = resultIter.next();
          if ((obj = checkObjectAuth(obj)) != null) {
            newResults.add(obj);
          }
        }
        if (results.isModifiable()) {
          results.clear();
          results.addAll(newResults);
        } else {
          ObjectType constraint = results.getCollectionType().getElementType();
          results = new ResultsCollectionWrapper(constraint, newResults);
          queryContext.setQueryResult(results);
        }
        return true;
      } else {
        return false;
      }
    }
    return false;
  }

  public void close() {

    this.principalName = null;
  }

}
