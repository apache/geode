/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.management.internal.security;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.security.Principal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import javax.management.remote.JMXPrincipal;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.operations.OperationContext;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.security.AccessControl;
import com.gemstone.gemfire.security.AuthenticationFailedException;
import com.gemstone.gemfire.security.Authenticator;
import com.gemstone.gemfire.security.NotAuthorizedException;

import org.apache.commons.io.IOUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class ExampleJSONAuthorization implements AccessControl, Authenticator {

  public static class Role {
    List<OperationContext> permissions = new ArrayList<>();
    String name;
    String serverGroup;
  }

  public static class User {
    String name;
    Set<Role> roles = new HashSet<>();
    String pwd;
  }

  private static Map<String, User> acl = null;

  public static ExampleJSONAuthorization create() throws IOException, JSONException {
    return new ExampleJSONAuthorization();
  }

  public ExampleJSONAuthorization() throws IOException, JSONException {
    setUpWithJsonFile("security.json");
  }

  public static void setUpWithJsonFile(String jsonFileName) throws IOException, JSONException {
    InputStream input = ExampleJSONAuthorization.class.getResourceAsStream(jsonFileName);
    if(input==null){
      throw new RuntimeException("Could not find resource " + jsonFileName);
    }

    StringWriter writer = new StringWriter();
    IOUtils.copy(input, writer, "UTF-8");
    String json = writer.toString();
    readSecurityDescriptor(json);
  }

  private static void readSecurityDescriptor(String json) throws IOException, JSONException {
    JSONObject jsonBean = new JSONObject(json);
    acl = new HashMap<>();
    Map<String, Role> roleMap = readRoles(jsonBean);
    readUsers(acl, jsonBean, roleMap);
  }

  private static void readUsers(Map<String, User> acl, JSONObject jsonBean, Map<String, Role> roleMap)
      throws JSONException {
    JSONArray array = jsonBean.getJSONArray("users");
    for (int i = 0; i < array.length(); i++) {
      JSONObject obj = array.getJSONObject(i);
      User user = new User();
      user.name = obj.getString("name");
      if (obj.has("password")) {
        user.pwd = obj.getString("password");
      } else {
        user.pwd = user.name;
      }

      JSONArray ops = obj.getJSONArray("roles");
      for (int j = 0; j < ops.length(); j++) {
        String roleName = ops.getString(j);
        user.roles.add(roleMap.get(roleName));
      }
      acl.put(user.name, user);
    }
  }

  private static Map<String, Role> readRoles(JSONObject jsonBean) throws JSONException {
    Map<String, Role> roleMap = new HashMap<>();
    JSONArray array = jsonBean.getJSONArray("roles");
    for (int i = 0; i < array.length(); i++) {
      JSONObject obj = array.getJSONObject(i);
      Role role = new Role();
      role.name = obj.getString("name");
      String regionNames = null;
      if(obj.has("regions")) {
        regionNames = obj.getString("regions");
      }
      JSONArray ops = obj.getJSONArray("operationsAllowed");
      for (int j = 0; j < ops.length(); j++) {
        String[] parts = ops.getString(j).split(":");
        if(regionNames!=null) {
          role.permissions.add(new ResourceOperationContext(parts[0], parts[1], regionNames));
        }
        else
          role.permissions.add(new ResourceOperationContext(parts[0], parts[1], "*"));
      }

      roleMap.put(role.name, role);

      if (obj.has("serverGroup")) {
        role.serverGroup = obj.getString("serverGroup");
      }
    }

    return roleMap;
  }

  public static Map<String, User> getAcl() {
    return acl;
  }

  private Principal principal = null;

  @Override
  public void close() {

  }

  @Override
  public boolean authorizeOperation(String region, OperationContext context) {
    if (principal == null)
      return false;

    User user = acl.get(principal.getName());
    if(user == null)
      return false; // this user is not authorized to do anything

    // check if the user has this permission defined in the context
    for(Role role:acl.get(user.name).roles) {
      for (OperationContext permitted : role.permissions) {
        if (permitted.implies(context)) {
          return true;
        }
      }
    }

    return false;
  }

  @Override
  public void init(Principal principal, DistributedMember arg1, Cache arg2) throws NotAuthorizedException {
    this.principal = principal;
  }

  @Override
  public Principal authenticate(Properties props, DistributedMember arg1) throws AuthenticationFailedException {
    String user = props.getProperty(ResourceConstants.USER_NAME);
    String pwd = props.getProperty(ResourceConstants.PASSWORD);
    User userObj = acl.get(user);
    if (userObj == null) throw new AuthenticationFailedException("Wrong username/password");
    LogService.getLogger().info("User=" + user + " pwd=" + pwd);
    if (user != null && !userObj.pwd.equals(pwd) && !"".equals(user))
      throw new AuthenticationFailedException("Wrong username/password");
    return new JMXPrincipal(user);
  }

  @Override
  public void init(Properties arg0, LogWriter arg1, LogWriter arg2) throws AuthenticationFailedException {

  }

}
