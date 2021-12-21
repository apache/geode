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
package org.apache.geode.internal.cache.execute;

import java.io.Serializable;
import java.util.Map;

/**
 * Arguments for a function need to be conditionalized on the target member. Pass an argument that
 * contains a default object as well as a map of member id to Serializable. When the function is
 * distributed, the* target member is looked up in this map by the function service. If the target
 * member id has a value in the map, then that value is used as an argument for that distribution.
 * If that member id is not found, then the default value is used as an argument instead.
 *
 * For the SQL Fabric, its use would be as follows: The defaultArgument is set to be the query
 * string. The keys of the map are set to be all known member ids that have already prepared this
 * statement, and the values are all null (to be interpreted as no argument).
 *
 * This way, when the function is distributed to a member that is not in the map, it will include
 * the query string as an argument. When it is sent to a member that is in the map, will not include
 * the query string as an argument.
 *
 * @since GemFire 6.0
 *
 */
public class MemberMappedArgument implements Serializable {

  private static final long serialVersionUID = -6465867775653599576L;

  private final Object defaultArgument;

  private final Map<String, Object> memberToArgMap;

  public MemberMappedArgument(Object defaultArgument, Map<String, Object> memberToArgMap) {
    this.defaultArgument = defaultArgument;
    this.memberToArgMap = memberToArgMap;
  }

  public Object getArgumentsForMember(String memberId) {
    if (memberToArgMap.containsKey(memberId)) {
      return memberToArgMap.get(memberId);
    } else {
      return defaultArgument;
    }
  }

  public Object getDefaultArgument() {
    return defaultArgument;
  }

  // TODO:Asif: Not good to return the refernec of the mapping. Should we return a copy?
  public Map<String, Object> getMemberSpecificArgumentsMap() {
    return memberToArgMap;
  }
}
