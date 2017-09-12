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
package org.apache.geode.security.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.geode.cache.query.internal.MethodInvocationAuthorizer;
import org.apache.geode.security.ResourcePermission;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;
import org.apache.geode.security.TestSecurityManager;

public class UserPermissions {

  // Must be the same as the region specified for the region specific users in the clientServer.json
  public static final String REGION_NAME = "region";

  public static List<String> RESTRICTED_METHOD_ACCESS_PERMISSIONS;
  
  public static final String SELECT_FROM_REGION_BY_FORMAT =
      "select * from /" + REGION_NAME + " r where r.%s = %s";
  public static final String SELECT_FORMAT_FROM_REGION = "select r.%s from /" + REGION_NAME + " r";
  public static final String SELECT_COUNT_OF_FORMAT_FROM_REGION =
      "select count(r.%s) from /" + REGION_NAME + " r";
  public static final String SELECT_MAX_OF_FORMAT_FROM_REGION =
      "select max(r.%s) from /" + REGION_NAME + " r";
  public static final String SELECT_MIN_OF_FORMAT_FROM_REGION =
      "select min(r.%s) from /" + REGION_NAME + " r";
  public static final String SELECT_FROM_REGION_WHERE_FORMAT_IN_REGION_FORMAT = "select * from /"
      + REGION_NAME + " r1 where r1.%1$s in (select r2.%1$s from /" + REGION_NAME + " r2)";
  public static final String SELECT_FROM_REGION_FORMAT = "select * from /" + REGION_NAME + ".%s";
  public static final String SELECT_FORMAT_FROM_REGION_ENTRYSET =
      "select e.%s from /" + REGION_NAME + ".entrySet e";

  public static final String SELECT_FROM_REGION_WITH_NUMERIC_METHOD =
      "select * from /" + REGION_NAME + " r where r.%s > 0";
  public static final String SELECT_NUMERIC_FROM_REGION_RESULTS =
      "select r.%s from /" + REGION_NAME + " r";

  // Regular ad hoc query
  public static final String SELECT_ALL_FROM_REGION = "select * from /" + REGION_NAME;
  // Testing public field getting
  public static final String SELECT_FROM_REGION_BY_PUBLIC_FIELD =
      String.format(SELECT_FROM_REGION_BY_FORMAT, "id", 1);
  public static final String SELECT_PUBLIC_FIELD_FROM_REGION =
      String.format(SELECT_FORMAT_FROM_REGION, "id");
  public static final String SELECT_COUNT_OF_PUBLIC_FIELD_FROM_REGION =
      String.format(SELECT_COUNT_OF_FORMAT_FROM_REGION, "id");
  public static final String SELECT_MAX_OF_PUBLIC_FIELD_FROM_REGION =
      String.format(SELECT_MAX_OF_FORMAT_FROM_REGION, "id");
  public static final String SELECT_MIN_OF_PUBLIC_FIELD_FROM_REGION =
      String.format(SELECT_MIN_OF_FORMAT_FROM_REGION, "id");
  public static final String SELECT_FROM_REGION_WHERE_PUBLIC_FIELD_IN_REGION_PUBLIC_FIELDS =
      String.format(SELECT_FROM_REGION_WHERE_FORMAT_IN_REGION_FORMAT, "id");

  // Testing getter invocation
  public static final String SELECT_FROM_REGION_BY_IMPLICIT_GETTER =
      String.format(SELECT_FROM_REGION_BY_FORMAT, "name", "'Beth'");
  public static final String SELECT_IMPLICIT_GETTER_FROM_REGION =
      String.format(SELECT_FORMAT_FROM_REGION, "name");
  public static final String SELECT_COUNT_OF_IMPLICIT_GETTER_FROM_REGION =
      String.format(SELECT_COUNT_OF_FORMAT_FROM_REGION, "name");
  public static final String SELECT_MAX_OF_IMPLICIT_GETTER_FROM_REGION =
      String.format(SELECT_MAX_OF_FORMAT_FROM_REGION, "name");
  public static final String SELECT_MIN_OF_IMPLICIT_GETTER_FROM_REGION =
      String.format(SELECT_MIN_OF_FORMAT_FROM_REGION, "name");
  public static final String SELECT_FROM_REGION1_WHERE_IMPLICIT_GETTER_IN_REGION2_IMPLICIT_GETTERS =
      String.format(SELECT_FROM_REGION_WHERE_FORMAT_IN_REGION_FORMAT, "name");
  public static final String SELECT_FROM_REGION_BY_DIRECT_GETTER =
      String.format(SELECT_FROM_REGION_BY_FORMAT, "getName", "'Beth'");
  public static final String SELECT_DIRECT_GETTER_FROM_REGION =
      String.format(SELECT_FORMAT_FROM_REGION, "getName");
  public static final String SELECT_COUNT_OF_DIRECT_GETTER_FROM_REGION =
      String.format(SELECT_COUNT_OF_FORMAT_FROM_REGION, "getName");
  public static final String SELECT_MAX_OF_DIRECT_GETTER_FROM_REGION =
      String.format(SELECT_MAX_OF_FORMAT_FROM_REGION, "getName");
  public static final String SELECT_MIN_OF_DIRECT_GETTER_FROM_REGION =
      String.format(SELECT_MIN_OF_FORMAT_FROM_REGION, "getName");
  public static final String SELECT_FROM_REGION_WHERE_DIRECT_GETTER_IN_REGION_DIRECT_GETTERS =
      String.format(SELECT_FROM_REGION_WHERE_FORMAT_IN_REGION_FORMAT, "getName");

  // Testing custom method invocation
  public static final String SELECT_FROM_REGION_BY_DEPLOYED_METHOD =
      String.format(SELECT_FROM_REGION_BY_FORMAT, "someMethod", "'John:1'");
  public static final String SELECT_DEPLOYED_METHOD_FROM_REGION =
      String.format(SELECT_FORMAT_FROM_REGION, "someMethod");
  public static final String SELECT_COUNT_OF_DEPLOYED_METHOD_FROM_REGION =
      String.format(SELECT_COUNT_OF_FORMAT_FROM_REGION, "someMethod");
  public static final String SELECT_MAX_OF_DEPLOYED_METHOD_FROM_REGION =
      String.format(SELECT_MAX_OF_FORMAT_FROM_REGION, "someMethod");
  public static final String SELECT_MIN_OF_DEPLOYED_METHOD_FROM_REGION =
      String.format(SELECT_MIN_OF_FORMAT_FROM_REGION, "someMethod");;
  public static final String SELECT_FROM_REGION_WHERE_DEPLOYED_METHOD_IN_REGION_DEPLOYED_METHODS =
      String.format(SELECT_FROM_REGION_WHERE_FORMAT_IN_REGION_FORMAT, "someMethod");

  // Geode method invocation
  public static final String SELECT_FROM_REGION_CONTAINS_KEY =
      String.format(SELECT_FROM_REGION_FORMAT, "containsKey('key')");
  public static final String SELECT_FROM_REGION_CONTAINS_VALUE =
      String.format(SELECT_FROM_REGION_FORMAT, "containsValue(15)");
  public static final String SELECT_FROM_REGION_CREATE =
      String.format(SELECT_FROM_REGION_FORMAT, "create('key2', 15)");
  public static final String SELECT_CREATE_FROM_REGION =
      String.format(SELECT_FORMAT_FROM_REGION, "create('key2', 15)");
  public static final String SELECT_FROM_REGION_DESTROY =
      String.format(SELECT_FROM_REGION_FORMAT, "destroy('key')");
  public static final String SELECT_FROM_REGION_DESTROY_REGION =
      String.format(SELECT_FROM_REGION_FORMAT, "destroyRegion");
  public static final String SELECT_DESTROY_REGION_FROM_REGION =
      String.format(SELECT_FORMAT_FROM_REGION, "destroyRegion");
  public static final String SELECT_FROM_REGION_DESTROY_REGION_PAREN =
      String.format(SELECT_FROM_REGION_FORMAT, "destroyRegion()");
  public static final String SELECT_DESTROY_REGION_PAREN_FROM_REGION =
      String.format(SELECT_FORMAT_FROM_REGION, "destroyRegion()");
  public static final String SELECT_FROM_REGION_GET =
      String.format(SELECT_FROM_REGION_FORMAT, "get('key')");
  public static final String SELECT_FROM_REGION_PUT =
      String.format(SELECT_FROM_REGION_FORMAT, "put('key2', 20)");
  public static final String SELECT_FROM_REGION_PUT_IF_ABSENT =
      String.format(SELECT_FROM_REGION_FORMAT, "putIfAbsent('key2', 20)");
  public static final String SELECT_FROM_REGION_REMOVE =
      String.format(SELECT_FROM_REGION_FORMAT, "remove('key')");
  public static final String SELECT_FROM_REGION_REPLACE =
      String.format(SELECT_FROM_REGION_FORMAT, "replace('key', 25)");
  public static final String SELECT_GET_INTEREST_LIST_REGEX_FROM_REGION =
      String.format(SELECT_FORMAT_FROM_REGION, "getInterestListRegex");
  public static final String SELECT_GET_INTEREST_LIST_REGEX_PAREN_FROM_REGION =
      String.format(SELECT_FORMAT_FROM_REGION, "getInterestListRegex()");

  // JDK method invocation
  public static final String SELECT_ADD_FROM_REGION_ENTRYSET =
      String.format(SELECT_FORMAT_FROM_REGION_ENTRYSET, "add('test')");
  public static final String SELECT_REMOVE_FROM_REGION_ENTRYSET =
      String.format(SELECT_FORMAT_FROM_REGION_ENTRYSET, "remove('key')");
  public static final String SELECT_FROM_REGION_TO_DATE =
      "SELECT * FROM /" + REGION_NAME + " where dateField = to_date('08/08/2018', 'MM/dd/yyyy')";

  // JDK numeric methods
  public static final String SELECT_INT_VALUE_FROM_REGION =
      String.format(SELECT_NUMERIC_FROM_REGION_RESULTS, "id.intValue");
  public static final String SELECT_LONG_VALUE_FROM_REGION =
      String.format(SELECT_NUMERIC_FROM_REGION_RESULTS, "id.longValue");
  public static final String SELECT_DOUBLE_VALUE_FROM_REGION =
      String.format(SELECT_NUMERIC_FROM_REGION_RESULTS, "id.doubleValue");
  public static final String SELECT_SHORT_VALUE_FROM_REGION =
      String.format(SELECT_NUMERIC_FROM_REGION_RESULTS, "id.shortValue");

  // Testing whitelisted methods
  public static final String SELECT_FROM_REGION_ENTRYSET =
      String.format(SELECT_FORMAT_FROM_REGION_ENTRYSET, "getKey, e.getValue");
  public static final String SELECT_FROM_REGION_VALUES =
      String.format(SELECT_FROM_REGION_FORMAT, "values");
  public static final String SELECT_FROM_REGION_KEYSET =
      String.format(SELECT_FROM_REGION_FORMAT, "keySet");
  public static final String SELECT_FROM_REGION_ENTRIES =
      "select e.getKey from /" + REGION_NAME + ".entries e";
  public static final String SELECT_FROM_REGION_ENTRY_SET =
      String.format(SELECT_FORMAT_FROM_REGION_ENTRYSET, "getKey");
  public static final String SELECT_KEY_FROM_REGION_ENTRYSET =
      String.format(SELECT_FORMAT_FROM_REGION_ENTRYSET, "key");
  public static final String SELECT_GET_KEY_FROM_REGION_ENTRYSET =
      String.format(SELECT_FORMAT_FROM_REGION_ENTRYSET, "getKey");
  public static final String SELECT_VALUE_FROM_REGION_ENTRYSET =
      String.format(SELECT_FORMAT_FROM_REGION_ENTRYSET, "value");
  public static final String SELECT_GET_VALUE_FROM_REGION_ENTRYSET =
      String.format(SELECT_FORMAT_FROM_REGION_ENTRYSET, "getValue");
  public static final String SELECT_FROM_REGION_BY_TO_STRING =
      String.format(SELECT_FROM_REGION_BY_FORMAT, "toString", "'Test_Object'");
  public static final String SELECT_TO_STRING_FROM_REGION =
      String.format(SELECT_FORMAT_FROM_REGION, "toString");
  public static final String SELECT_FROM_REGION_BY_TO_STRING_TO_UPPER_CASE =
      String.format(SELECT_FROM_REGION_BY_FORMAT, "toString.toUpperCase", "'TEST_OBJECT'");
  public static final String SELECT_FROM_REGION_BY_TO_STRING_TO_LOWER_CASE =
      String.format(SELECT_FROM_REGION_BY_FORMAT, "toString.toLowerCase", "'test_object'");

  // Testing blocked methods
  public static final String SELECT_FROM_REGION_BY_GET_CLASS =
      String.format(SELECT_FROM_REGION_BY_FORMAT, "getClass", "'org.apache'");
  public static final String SELECT_FROM_REGION_BY_GET_CLASS_PAREN =
      String.format(SELECT_FROM_REGION_BY_FORMAT, "getClass()", "'org.apache'");
  public static final String SELECT_GET_CLASS_FROM_REGION =
      String.format(SELECT_FORMAT_FROM_REGION, "getClass");
  public static final String SELECT_GET_CLASS_PAREN_FROM_REGION =
      String.format(SELECT_FORMAT_FROM_REGION, "getClass()");
  public static final String SELECT_FROM_REGION_BY_CLASS =
      String.format(SELECT_FROM_REGION_BY_FORMAT, "class", "'org.apache'");
  public static final String SELECT_CLASS_FROM_REGION =
      String.format(SELECT_FORMAT_FROM_REGION, "class");
  public static final String SELECT_FROM_REGION_BY_CAPITAL_CLASS =
      String.format(SELECT_FROM_REGION_BY_FORMAT, "Class", "'org.apache'");
  public static final String SELECT_CAPITAL_CLASS_FROM_REGION =
      String.format(SELECT_FORMAT_FROM_REGION, "Class");
  public static final String SELECT_FROM_REGION_GET_CLASS =
      String.format(SELECT_FROM_REGION_FORMAT, "getClass");
  public static final String SELECT_FROM_REGION_GET_CLASS_PAREN =
      String.format(SELECT_FROM_REGION_FORMAT, "getClass()");
  public static final String SELECT_FROM_REGION_CLASS =
      String.format(SELECT_FROM_REGION_FORMAT, "class");
  public static final String SELECT_FROM_REGION_CAPITAL_CLASS =
      String.format(SELECT_FROM_REGION_FORMAT, "Class");
  public static final String SELECT_FROM_REGION_CLONE =
      String.format(SELECT_FROM_REGION_FORMAT, "clone");
  public static final String SELECT_FROM_REGION_WAIT =
      String.format(SELECT_FROM_REGION_FORMAT, "wait");
  public static final String SELECT_CLONE_FROM_REGION =
      String.format(SELECT_FORMAT_FROM_REGION, "clone");
  public static final String SELECT_WAIT_FROM_REGION =
      String.format(SELECT_FORMAT_FROM_REGION, "wait");


  private TestSecurityManager manager = new TestSecurityManager();
  public HashMap<String, List<String>> queryPermissions;

  public UserPermissions(String requiredSecurityTag) {
    this(Arrays.asList(requiredSecurityTag));
  }

  public UserPermissions(List<String> requiredSecurityTags) {
    if (!manager.initializeFromJsonResource(
        "org/apache/geode/management/internal/security/clientServer.json"))
      throw new RuntimeException(
          "Something bad happened while trying to load the TestSecurityManager with the org/apache/geode/management/internal/security/clientServer.json");
    queryPermissions = initQueryPermissions(requiredSecurityTags);
  }

  public ResourcePermission createResourcePermissionFromString(String perm) {
    String[] permParts = perm.split(":");

    Resource res = Resource.valueOf(permParts[0]);
    Operation op = Operation.valueOf(permParts[1]);
    if (permParts.length == 2)
      return new ResourcePermission(res, op);
    else {
      String target = permParts[3];
      if (permParts.length == 3)
        return new ResourcePermission(res, op, target);
      else {
        String targetKey = permParts[4];
        return new ResourcePermission(res, op, target, targetKey);
      }
    }
  }

  public List<ResourcePermission> createResourcePermissionListFromStrings(String... perms) {
    List<ResourcePermission> resPerms = new ArrayList<>();
    for (String perm : perms) {
      resPerms.add(createResourcePermissionFromString(perm));
    }
    return resPerms;
  }

  public HashMap<String, List<String>> initQueryPermissions(List<String> requiredSecurityTags) {
    HashMap<String, List<String>> perms = new HashMap<>();

    String securityString = "";
    Iterator<String> iterator = requiredSecurityTags.iterator();
    while (iterator.hasNext()) {
      securityString += "(.*" + iterator.next();
      if (iterator.hasNext()) {
        securityString += ".*)|";
      }
    }

    RESTRICTED_METHOD_ACCESS_PERMISSIONS = Arrays.asList(
        MethodInvocationAuthorizer.UNAUTHORIZED_STRING + ".*)|" + securityString);
    perms.put(SELECT_ALL_FROM_REGION, RESTRICTED_METHOD_ACCESS_PERMISSIONS);

    // Public field access
    perms.put(SELECT_FROM_REGION_BY_PUBLIC_FIELD, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_PUBLIC_FIELD_FROM_REGION, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_COUNT_OF_PUBLIC_FIELD_FROM_REGION, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_MAX_OF_PUBLIC_FIELD_FROM_REGION, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_MIN_OF_PUBLIC_FIELD_FROM_REGION, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_FROM_REGION_WHERE_PUBLIC_FIELD_IN_REGION_PUBLIC_FIELDS,
        RESTRICTED_METHOD_ACCESS_PERMISSIONS);

    // Getter access
    perms.put(SELECT_FROM_REGION_BY_IMPLICIT_GETTER, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_IMPLICIT_GETTER_FROM_REGION, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_COUNT_OF_IMPLICIT_GETTER_FROM_REGION, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_MAX_OF_IMPLICIT_GETTER_FROM_REGION, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_MIN_OF_IMPLICIT_GETTER_FROM_REGION, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_FROM_REGION1_WHERE_IMPLICIT_GETTER_IN_REGION2_IMPLICIT_GETTERS,
        RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_FROM_REGION_BY_DIRECT_GETTER, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_DIRECT_GETTER_FROM_REGION, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_COUNT_OF_DIRECT_GETTER_FROM_REGION, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_MAX_OF_DIRECT_GETTER_FROM_REGION, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_MIN_OF_DIRECT_GETTER_FROM_REGION, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_FROM_REGION_WHERE_DIRECT_GETTER_IN_REGION_DIRECT_GETTERS,
        RESTRICTED_METHOD_ACCESS_PERMISSIONS);

    // Deployed Methods
    perms.put(SELECT_FROM_REGION_BY_DEPLOYED_METHOD, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_DEPLOYED_METHOD_FROM_REGION, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_COUNT_OF_DEPLOYED_METHOD_FROM_REGION, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_MAX_OF_DEPLOYED_METHOD_FROM_REGION, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_MIN_OF_DEPLOYED_METHOD_FROM_REGION, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_FROM_REGION_WHERE_DEPLOYED_METHOD_IN_REGION_DEPLOYED_METHODS,
        RESTRICTED_METHOD_ACCESS_PERMISSIONS);

    // Gemfire Methods
    perms.put(SELECT_FROM_REGION_CONTAINS_KEY, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_FROM_REGION_CONTAINS_VALUE, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_FROM_REGION_CREATE, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_CREATE_FROM_REGION, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_FROM_REGION_DESTROY, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_FROM_REGION_DESTROY_REGION, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_DESTROY_REGION_FROM_REGION, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_FROM_REGION_DESTROY_REGION_PAREN, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_DESTROY_REGION_PAREN_FROM_REGION, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_FROM_REGION_GET, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_FROM_REGION_PUT, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_FROM_REGION_PUT_IF_ABSENT, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_FROM_REGION_REMOVE, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_FROM_REGION_REPLACE, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_GET_INTEREST_LIST_REGEX_FROM_REGION, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_GET_INTEREST_LIST_REGEX_PAREN_FROM_REGION,
        RESTRICTED_METHOD_ACCESS_PERMISSIONS);

    // JDK Methods
    perms.put(SELECT_ADD_FROM_REGION_ENTRYSET, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_REMOVE_FROM_REGION_ENTRYSET, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_FROM_REGION_TO_DATE, RESTRICTED_METHOD_ACCESS_PERMISSIONS);

    // Numeric Methods
    perms.put(SELECT_INT_VALUE_FROM_REGION, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_LONG_VALUE_FROM_REGION, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_DOUBLE_VALUE_FROM_REGION, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_SHORT_VALUE_FROM_REGION, RESTRICTED_METHOD_ACCESS_PERMISSIONS);

    // Whitelisted Methods
    perms.put(SELECT_FROM_REGION_ENTRYSET, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_FROM_REGION_VALUES, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_FROM_REGION_KEYSET, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_FROM_REGION_ENTRIES, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_FROM_REGION_ENTRY_SET, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_KEY_FROM_REGION_ENTRYSET, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_GET_KEY_FROM_REGION_ENTRYSET, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_VALUE_FROM_REGION_ENTRYSET, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_GET_VALUE_FROM_REGION_ENTRYSET, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_FROM_REGION_BY_TO_STRING, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_TO_STRING_FROM_REGION, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_FROM_REGION_BY_TO_STRING_TO_UPPER_CASE, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_FROM_REGION_BY_TO_STRING_TO_LOWER_CASE, RESTRICTED_METHOD_ACCESS_PERMISSIONS);

    // Blacklisted Methods
    perms.put(SELECT_FROM_REGION_BY_GET_CLASS, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_FROM_REGION_BY_GET_CLASS_PAREN, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_GET_CLASS_FROM_REGION, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_GET_CLASS_PAREN_FROM_REGION, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_FROM_REGION_BY_CLASS, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_CLASS_FROM_REGION, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_FROM_REGION_BY_CAPITAL_CLASS, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_CAPITAL_CLASS_FROM_REGION, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_FROM_REGION_GET_CLASS, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_FROM_REGION_GET_CLASS_PAREN, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_FROM_REGION_CLASS, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_FROM_REGION_CAPITAL_CLASS, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_FROM_REGION_CLONE, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_FROM_REGION_WAIT, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_CLONE_FROM_REGION, RESTRICTED_METHOD_ACCESS_PERMISSIONS);
    perms.put(SELECT_WAIT_FROM_REGION, RESTRICTED_METHOD_ACCESS_PERMISSIONS);

    return perms;
  }

  public static List<String> getAllUsers() {
    return Arrays.asList("stranger", "dataReader", "dataReaderRegion", "dataReaderRegionKey",
        "clusterManager", "clusterManagerQuery", "clusterManagerDataReader",
        "clusterManagerDataReaderRegion", "clusterManagerDataReaderRegionKey", "super-user"); // Includes
                                                                                              // all
                                                                                              // cluster
                                                                                              // permissions
                                                                                              // (READ,
                                                                                              // WRITE,
                                                                                              // MANAGE)
                                                                                              // as
                                                                                              // well
                                                                                              // as
                                                                                              // all
                                                                                              // data
                                                                                              // privileges
                                                                                              // (READ,
                                                                                              // WRITE,
                                                                                              // MANAGE)
  }

  private List<String> getUserResourcePermissions(String user) {
    List<String> userPerms = new ArrayList<>();
    Set<TestSecurityManager.Role> userRoles = manager.getUser(user).getRoles();

    for (TestSecurityManager.Role role : userRoles) {
      for (ResourcePermission perm : role.getPermissions()) {
        userPerms.add(perm.toString());
      }
    }
    return userPerms;
  }

  public List<String> getMissingUserAuthorizationsForQuery(String user, String query) {
    List<String> foundPerms = new ArrayList<>();
    List<String> userPerms = getUserResourcePermissions(user);
    List<String> reqPerms = queryPermissions.get(query);

    System.out.println("userPerms: " + userPerms);
    System.out.println("reqPerms: " + reqPerms);

    for (String reqPerm : reqPerms) {
      for (String userPerm : userPerms) {
        if (userPerm.equals(reqPerm)) {
          foundPerms.add(reqPerm);
        } else {
          String[] reqPermParts = reqPerm.split(":");
          String[] userPermParts = userPerm.split(":");
          if (reqPermParts.length > userPermParts.length) {
            boolean hasGenericPerm = true;
            for (int i = 0; i < userPermParts.length; i++) {
              if (!reqPermParts[i].equals(userPermParts[i])) {
                hasGenericPerm = false;
                break;
              }
            }

            if (hasGenericPerm) {
              foundPerms.add(reqPerm);
            }
          }
        }
      }
    }

    List<String> missingPerms = new ArrayList<>(reqPerms);
    missingPerms.removeAll(foundPerms);
    System.out.println("Missing perms: " + missingPerms);
    return missingPerms;
  }

  public String getUserPassword(String user) {
    return manager.getUser(user).getPassword();
  }

  public String regexForExpectedException(List<String> expectedExceptionMessages) {
    String authErrorRegexp = "(.*" + String.join(".*)|(.*", expectedExceptionMessages) + ".*)";
    return authErrorRegexp;
  }

}
