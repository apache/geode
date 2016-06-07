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
package com.gemstone.gemfire.security.templates;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.operations.OperationContext;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.management.internal.security.ResourceConstants;
import com.gemstone.gemfire.management.internal.security.ResourceOperationContext;
import com.gemstone.gemfire.security.AccessControl;
import com.gemstone.gemfire.security.AuthenticationFailedException;
import com.gemstone.gemfire.security.Authenticator;
import com.gemstone.gemfire.security.NotAuthorizedException;
import org.apache.commons.io.IOUtils;

import javax.management.remote.JMXPrincipal;
import java.io.File;
import java.io.FileReader;
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
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * This class provides a sample implementation for authentication and authorization via the {@link AccessControl}
 * and {@link Authenticator} interfaces.
 *
 * In order to use it, a Geode member must be started with the following properties:
 * <p/>
 * <code>
 *   security-client-authenticator = com.gemstone.gemfire.security.examples.SampleJsonAuthorization.create
 *   security-client-accessor = com.gemstone.gemfire.security.examples.SampleJsonAuthorization.create
 * </code>
 * <p/>
 * The class is initialized with a JSON file called {@code security.json}. This file must exist on the classpath,
 * so members should be started with an appropriate {@code --classpath} option.
 * <p/>
 * The format of the file is as follows:
 * <pre>
 * {
 *   "roles": [
 *     {
 *       "name": "admin",
 *       "operationsAllowed": [
 *         "CLUSTER:MANAGE",
 *         "DATA:MANAGE"
 *       ]
 *     },
 *     {
 *       "name": "readRegionA",
 *       "operationsAllowed": [
 *         "DATA:READ"
 *       ],
 *       "regions": ["RegionA", "RegionB"]
 *     }
 *   ]
 *   "users": [
 *     {
 *       "name": "admin",
 *       "password": "secret".
 *       "roles": ["admin"]
 *     },
 *     {
 *       "name": "guest",
 *       "password": "guest",
 *       "roles": ["readRegionA"]
 *     }
 *   ]
 * }
 * </pre>
 */
public class SampleJsonAuthorization implements AccessControl, Authenticator {

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

  public static SampleJsonAuthorization create() throws IOException {
    if (acl == null) {
      setUpWithJsonFile("security.json");
    }
    return new SampleJsonAuthorization();
  }

  public static void setUpWithJsonFile(String jsonFileName) throws IOException {
    InputStream input = ClassLoader.getSystemResourceAsStream(jsonFileName);
    if (input == null) {
      throw new RuntimeException("Could not find the required JSON security file on the classpath: " + jsonFileName);
    }

    StringWriter writer = new StringWriter();
    IOUtils.copy(input, writer, "UTF-8");
    String json = writer.toString();
    readSecurityDescriptor(json);
  }

  protected static void readSecurityDescriptor(String json) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode jsonNode = mapper.readTree(json);
    acl = new HashMap<>();
    Map<String, Role> roleMap = readRoles(jsonNode);
    readUsers(acl, jsonNode, roleMap);
  }

  private static void readUsers(Map<String, User> acl, JsonNode node, Map<String, Role> roleMap) {
    for (JsonNode u : node.get("users")) {
      User user = new User();
      user.name = u.get("name").asText();

      if (u.has("password")) {
        user.pwd = u.get("password").asText();
      } else {
        user.pwd = user.name;
      }

      for (JsonNode r : u.get("roles")) {
        user.roles.add(roleMap.get(r.asText()));
      }

      acl.put(user.name, user);
    }
  }

  private static Map<String, Role> readRoles(JsonNode jsonNode) {
    Map<String, Role> roleMap = new HashMap<>();
    for (JsonNode r : jsonNode.get("roles")) {
      Role role = new Role();
      role.name = r.get("name").asText();
      String regionNames = null;

      JsonNode regions = r.get("regions");
      if (regions != null) {
        if (regions.isArray()) {
          regionNames = StreamSupport.stream(regions.spliterator(), false)
              .map(JsonNode::asText)
              .collect(Collectors.joining(","));
        } else {
          regionNames = regions.asText();
        }
      }

      for (JsonNode op : r.get("operationsAllowed")) {
        String[] parts = op.asText().split(":");
        String resourcePart = (parts.length > 0) ? parts[0] : null;
        String operationPart = (parts.length > 1) ? parts[1] : null;
        String regionPart = (regionNames != null) ? regionNames : "*";
        role.permissions.add(new ResourceOperationContext(resourcePart, operationPart, regionPart));
      }

      roleMap.put(role.name, role);

      if (r.has("serverGroup")) {
        role.serverGroup = r.get("serverGroup").asText();
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
    if (principal == null) return false;

    User user = acl.get(principal.getName());
    if (user == null) return false; // this user is not authorized to do anything

    // check if the user has this permission defined in the context
    for (Role role : acl.get(user.name).roles) {
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
    if (userObj == null) {
      throw new AuthenticationFailedException("Wrong username/password");
    }

    LogService.getLogger().info("User=" + user + " pwd=" + pwd);
    if (user != null && !userObj.pwd.equals(pwd) && !"".equals(user)) {
      throw new AuthenticationFailedException("Wrong username/password");
    }

    return new JMXPrincipal(user);
  }

  @Override
  public void init(Properties arg0, LogWriter arg1, LogWriter arg2) throws AuthenticationFailedException {
  }

  protected static String readFile(String name) throws IOException {
    File file = new File(name);
    FileReader reader = new FileReader(file);
    char[] buffer = new char[(int) file.length()];
    reader.read(buffer);
    String json = new String(buffer);
    reader.close();
    return json;
  }
}
