/*
 * ========================================================================= 
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved. 
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 * =========================================================================
 */
package com.gemstone.gemfire.internal.cache.execute;

import java.io.Serializable;
import java.util.Map;

/**
 * Arguments for a function need to be conditionalized on the target member.
 * Pass an argument that contains a default object as well as a map of member id to
 * Serializable. When the function is distributed, the* target member is looked up in
 * this map by the function service. If the target member id has a value in the
 * map, then that value is used as an argument for that distribution. If that
 * member id is not found, then the default value is used as an argument
 * instead.
 * 
 * For the SQL Fabric, its use would be as follows: The defaultArgument is set
 * to be the query string. The keys of the map are set to be all known member
 * ids that have already prepared this statement, and the values are all null
 * (to be interpreted as no argument).
 * 
 * This way, when the function is distributed to a member that is not in the
 * map, it will include the query string as an argument. When it is sent to a
 * member that is in the map, will not include the query string as an argument.
 * 
 * @author Yogesh Mahajan
 * @since 6.0
 * 
 */
public class MemberMappedArgument implements Serializable{

  private static final long serialVersionUID = -6465867775653599576L;

  private Object defaultArgument;

  private Map<String, Object> memberToArgMap;

  public MemberMappedArgument(Object defaultArgument,
      Map<String, Object> memberToArgMap) {
    this.defaultArgument = defaultArgument;
    this.memberToArgMap = memberToArgMap;
  }

  public Object getArgumentsForMember(String memberId) {
    if (memberToArgMap.containsKey(memberId)) {
      return memberToArgMap.get(memberId);
    }
    else {
      return this.defaultArgument;
    }
  }
  
  public Object getDefaultArgument() {
    return this.defaultArgument;
  }
  //TODO:Asif: Not good to return the refernec of the mapping. Should we return a copy?
  public Map<String, Object> getMemberSpecificArgumentsMap() {
    return this.memberToArgMap;
  }
}
