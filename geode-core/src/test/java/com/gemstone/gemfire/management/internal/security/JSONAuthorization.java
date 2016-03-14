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

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.operations.OperationContext;
import com.gemstone.gemfire.cache.operations.OperationContext.OperationCode;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.security.AccessControl;
import com.gemstone.gemfire.security.AuthenticationFailedException;
import com.gemstone.gemfire.security.Authenticator;
import com.gemstone.gemfire.security.NotAuthorizedException;
import com.gemstone.gemfire.util.test.TestUtil;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.management.remote.JMXPrincipal;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class JSONAuthorization implements AccessControl, Authenticator {

  static class Permission {

    private final Resource resource;
    private final OperationCode operationCode;

    Permission(Resource resource, OperationCode operationCode) {
      this.resource = resource;
      this.operationCode = operationCode;
    }

    public Resource getResource() {
      return resource;
    }

    public OperationCode getOperationCode() {
      return operationCode;
    }

    @Override
    public String toString() {
      String result = resource.toString() + ":" + operationCode.toString();
      return result;
    }
  }

  public static class Role {
    List<Permission> permissions = new ArrayList<>();
    String name;
    List<String> regionNames = null; // when checking, if regionNames is null, that means all regions are allowed.
    String serverGroup;
  }

  public static class User {
    String name;
    Set<Role> roles = new HashSet<>();
    String pwd;
  }

  private static Map<String, User> acl = null;

  public static JSONAuthorization create() throws IOException, JSONException {
    return new JSONAuthorization();
  }

  public JSONAuthorization() {
  }

  public JSONAuthorization(String jsonFileName) throws IOException, JSONException {
    setUpWithJsonFile(jsonFileName);
  }

  public static void setUpWithJsonFile(String jsonFileName) throws IOException, JSONException {
    String json = readFile(TestUtil.getResourcePath(JSONAuthorization.class, jsonFileName));
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
      JSONArray ops = obj.getJSONArray("operationsAllowed");
      for (int j = 0; j < ops.length(); j++) {
        String[] parts = ops.getString(j).split(":");
        Resource r = Resource.valueOf(parts[0]);
        OperationCode op = parts.length > 1 ? OperationCode.valueOf(parts[1]) : OperationCode.ALL;
        role.permissions.add(new Permission(r, op));
      }

      if(obj.has("region")) {
        if (role.regionNames == null) {
          role.regionNames = new ArrayList<>();
        }
        role.regionNames.add(obj.getString("region"));
      }

      if(obj.has("regions")) {
        JSONArray regions = obj.getJSONArray("regions");
        if (role.regionNames == null) {
          role.regionNames = new ArrayList<>();
        }
        for (int j = 0; j < regions.length(); j++) {
          role.regionNames.add(regions.getString(j));
        }
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
  public boolean authorizeOperation(String arg0, OperationContext context) {
    if (principal == null)
      return false;

    User user = acl.get(principal.getName());
    if(user == null)
      return false; // this user is not authorized to do anything

    LogService.getLogger().info("Context received " + context);
    LogService.getLogger().info("Checking for permission " + context.getResource() + ":" + context.getOperationCode());

    // check if the user has this permission defined in the context
    for(Role role:acl.get(user.name).roles) {
      for (Permission perm : role.permissions) {
        if (context.getResource() == perm.getResource() && context.getOperationCode() == perm.getOperationCode()) {
          LogService.getLogger().info("Found permission " + perm);

          //if this is only for JMX aurthorization, we've found the permission needed, i.e, this operation is authorized
          if(!(context instanceof CLIOperationContext)){
            return true;
          }

          // If this is a Command operation context, we need to further check if the region is allowed in this role
          CLIOperationContext ctx = (CLIOperationContext) context;
          String region = ctx.getCommandOptions().get("region");
          if(role.regionNames == null || role.regionNames.contains(region)){
            // if regionName is null, i.e. all regions are allowed
            return true;
          }
        }
      }
    }

    LogService.getLogger().info("Did not find code " + context.getOperationCode());
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
    LogService.getLogger().info("Authentication successful!! for " + user);
    return new JMXPrincipal(user);
  }

  @Override
  public void init(Properties arg0, LogWriter arg1, LogWriter arg2) throws AuthenticationFailedException {

  }

  private static String readFile(String name) throws IOException, JSONException {
    File file = new File(name);
    FileReader reader = new FileReader(file);
    char[] buffer = new char[(int) file.length()];
    reader.read(buffer);
    String json = new String(buffer);
    reader.close();
    return json;
  }
}
