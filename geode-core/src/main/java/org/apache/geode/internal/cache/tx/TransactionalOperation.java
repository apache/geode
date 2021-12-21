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
package org.apache.geode.internal.cache.tx;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TransactionalOperation {
  public enum ServerRegionOperation {
    CONTAINS_KEY,
    CONTAINS_VALUE,
    CONTAINS_VALUE_FOR_KEY,
    DESTROY, // includes REMOVE(k,v)
    EXECUTE_FUNCTION,
    GET,
    GET_ENTRY,
    GET_ALL,
    INVALIDATE,
    KEY_SET,
    PUT, // includes PUT_IF_ABSENT
    PUT_ALL,
    REMOVE_ALL;

    /**
     * @return true if the key associated with this op should be locked on clients locally; false
     *         otherwise
     */
    public static boolean lockKeyForTx(ServerRegionOperation op) {
      return op == PUT || op == PUT_ALL || op == REMOVE_ALL || op == DESTROY || op == INVALIDATE;
    }
  }

  // private ClientTXStateStub clienttx;
  protected ServerRegionOperation operation;
  protected String regionName;
  protected Object key;
  protected Object[] arguments;

  protected TransactionalOperation(ClientTXStateStub clienttx, String regionName,
      ServerRegionOperation op, Object key, Object[] arguments) {
    // this.clienttx = clienttx;
    this.regionName = regionName;
    operation = op;
    this.key = key;
    this.arguments = arguments;
  }

  /**
   * @return the name of the affected region
   */
  public String getRegionName() {
    return regionName;
  }

  public ServerRegionOperation getOperation() {
    return operation;
  }

  public Object getKey() {
    return key;
  }

  /**
   * @return set of keys if the operation was PUTALL or REMOVEALL; null otherwise
   */
  public Set<Object> getKeys() {
    if (operation == ServerRegionOperation.PUT_ALL) {
      Map m = (Map) arguments[0];
      return m.keySet();
    } else if (operation == ServerRegionOperation.REMOVE_ALL) {
      Collection<Object> keys = (Collection<Object>) arguments[0];
      return new HashSet(keys);
    }
    return null;
  }

  /** this returns internal arguments for reinvoking methods on the ServerRegionProxy */
  public Object[] getArguments() {
    return arguments;
  }

  @Override
  public String toString() {
    return "TransactionalOperation(region=" + regionName + "; op=" + operation + "; key="
        + key + ")";
  }

}
