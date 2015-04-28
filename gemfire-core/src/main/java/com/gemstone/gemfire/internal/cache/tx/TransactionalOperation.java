/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 * 
 */
package com.gemstone.gemfire.internal.cache.tx;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Bruce Schuchardt
 *
 */
public class TransactionalOperation {
  static public enum ServerRegionOperation {
    CONTAINS_KEY,
    CONTAINS_VALUE,
    CONTAINS_VALUE_FOR_KEY,
    DESTROY,  // includes REMOVE(k,v)
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
     * @param op
     * @return true if the key associated with this op should be locked on
     * clients locally; false otherwise
     */
    public static boolean lockKeyForTx(ServerRegionOperation op) {
      if (op == PUT || op == PUT_ALL || op == REMOVE_ALL || op == DESTROY || op == INVALIDATE) {
        return true;
      }
      return false;
    }
    };

//  private ClientTXStateStub clienttx;
  private ServerRegionOperation operation;
  private String regionName;
  private Object key;
  private Object[] arguments;

  protected TransactionalOperation(ClientTXStateStub clienttx, String regionName, ServerRegionOperation op, Object key, Object arguments[]) {
//    this.clienttx = clienttx;
    this.regionName = regionName;
    this.operation = op;
    this.key = key;
    this.arguments = arguments;
  }
  
  /**
   * @return the name of the affected region
   */
  public String getRegionName() {
    return this.regionName;
  }
  
  public ServerRegionOperation getOperation() {
    return this.operation;
  }
  
  public Object getKey() {
    return this.key;
  }
  
  /**
   * @return set of keys if the operation was PUTALL or REMOVEALL; null otherwise
   */
  public Set<Object> getKeys() {
    if (this.operation == ServerRegionOperation.PUT_ALL) {
      Map m = (Map) this.arguments[0];
      return m.keySet();
    } else if (this.operation == ServerRegionOperation.REMOVE_ALL) {
      Collection<Object> keys = (Collection<Object>) this.arguments[0];
      return new HashSet(keys);
    }
    return null;
  }
  
  /** this returns internal arguments for reinvoking methods on the ServerRegionProxy */
  public Object[] getArguments() {
    return this.arguments;
  }
  
  @Override
  public String toString() {
    return "TransactionalOperation(region="+this.regionName+"; op="+this.operation
     + "; key=" + this.key + ")";
  }

}
