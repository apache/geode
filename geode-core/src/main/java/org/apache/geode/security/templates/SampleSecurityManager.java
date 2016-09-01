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
package org.apache.geode.security.templates;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.geode.security.ResourcePermission;
import org.apache.geode.security.SecurityManager;
import org.apache.shiro.authz.Permission;

import com.gemstone.gemfire.management.internal.security.ResourceConstants;
import com.gemstone.gemfire.security.AuthenticationFailedException;
import com.gemstone.gemfire.security.NotAuthorizedException;

/**
 * This class provides a sample implementation of {@link SecurityManager} for
 * authentication and authorization initialized from data provided as JSON.
 * <p>
 * <p>A Geode member must be configured with the following:
 * <p>
 * <p>{@code security-manager = com.gemstone.gemfire.security.examples.SampleSecurityManager}
 * <p>
 * <p>The class can be initialized with from a JSON resource called
 * {@code security.json}. This file must exist on the classpath, so members
 * should be started with an appropriate {@code --classpath} option.
 * <p>
 * <p>The format of the JSON for configuration is as follows:
 * <pre><code>
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
 *   ],
 *   "users": [
 *     {
 *       "name": "admin",
 *       "password": "secret",
 *       "roles": ["admin"]
 *     },
 *     {
 *       "name": "guest",
 *       "password": "guest",
 *       "roles": ["readRegionA"]
 *     }
 *   ]
 * }
 * </code></pre>
 */
public class SampleSecurityManager implements SecurityManager {

  public static final String SECURITY_JSON = "security-json";

  protected static final String DEFAULT_JSON_FILE_NAME = "security.json";

  private Map<String, User> userNameToUser;

  @Override
  public boolean authorize(final Object principal, final ResourcePermission context) {
    if (principal == null) return false;

    User user = this.userNameToUser.get(principal.toString());
    if (user == null) {
      return false; // this user is not authorized to do anything
    }

    // check if the user has this permission defined in the context
    for (Role role : this.userNameToUser.get(user.name).roles) {
      for (Permission permitted : role.permissions) {
        if (permitted.implies(context)) {
          return true;
        }
      }
    }

    return false;
  }

  @Override
  public void init(final Properties securityProperties) throws NotAuthorizedException {
    String jsonPropertyValue = securityProperties != null ? securityProperties.getProperty(SECURITY_JSON) : null;
    if (jsonPropertyValue == null) {
      jsonPropertyValue = DEFAULT_JSON_FILE_NAME;
    }

    if (!initializeFromJsonResource(jsonPropertyValue)) {
      throw new AuthenticationFailedException("SampleSecurityManager: unable to find json resource \"" + jsonPropertyValue + "\" as specified by [" + SECURITY_JSON + "].");
    }
  }

  @Override
  public Object authenticate(final Properties credentials) throws AuthenticationFailedException {
    String user = credentials.getProperty(ResourceConstants.USER_NAME);
    String password = credentials.getProperty(ResourceConstants.PASSWORD);

    User userObj = this.userNameToUser.get(user);
    if (userObj == null) {
      throw new AuthenticationFailedException("SampleSecurityManager: wrong username/password");
    }

    if (user != null && !userObj.password.equals(password) && !"".equals(user)) {
      throw new AuthenticationFailedException("SampleSecurityManager: wrong username/password");
    }

    return user;
  }

  boolean initializeFromJson(final String json) {
    try {
      ObjectMapper mapper = new ObjectMapper();
      JsonNode jsonNode = mapper.readTree(json);
      this.userNameToUser = new HashMap<>();
      Map<String, Role> roleMap = readRoles(jsonNode);
      readUsers(this.userNameToUser, jsonNode, roleMap);
      return true;
    } catch (IOException ex) {
      return false;
    }
  }

  boolean initializeFromJsonResource(final String jsonResource) {
    try {
      InputStream input = ClassLoader.getSystemResourceAsStream(jsonResource);
      if (input != null) {
        initializeFromJson(readJsonFromInputStream(input));
        return true;
      }
    } catch (IOException ex) {
    }
    return false;
  }

  public User getUser(final String user) {
    return this.userNameToUser.get(user);
  }

  private String readJsonFromInputStream(final InputStream input) throws IOException {
    StringWriter writer = new StringWriter();
    IOUtils.copy(input, writer, "UTF-8");
    return writer.toString();
  }

  private void readUsers(final Map<String, User> rolesToUsers, final JsonNode node, final Map<String, Role> roleMap) {
    for (JsonNode usersNode : node.get("users")) {
      User user = new User();
      user.name = usersNode.get("name").asText();

      if (usersNode.has("password")) {
        user.password = usersNode.get("password").asText();
      } else {
        user.password = user.name;
      }

      for (JsonNode rolesNode : usersNode.get("roles")) {
        user.roles.add(roleMap.get(rolesNode.asText()));
      }

      rolesToUsers.put(user.name, user);
    }
  }

  private Map<String, Role> readRoles(final JsonNode jsonNode) {
    if (jsonNode.get("roles") == null) {
      return Collections.EMPTY_MAP;
    }
    Map<String, Role> roleMap = new HashMap<>();
    for (JsonNode rolesNode : jsonNode.get("roles")) {
      Role role = new Role();
      role.name = rolesNode.get("name").asText();
      String regionNames = null;
      String keys = null;

      JsonNode regionsNode = rolesNode.get("regions");
      if (regionsNode != null) {
        if (regionsNode.isArray()) {
          regionNames = StreamSupport.stream(regionsNode.spliterator(), false)
                                     .map(JsonNode::asText)
                                     .collect(Collectors.joining(","));
        } else {
          regionNames = regionsNode.asText();
        }
      }

      for (JsonNode operationsAllowedNode : rolesNode.get("operationsAllowed")) {
        String[] parts = operationsAllowedNode.asText().split(":");
        String resourcePart = (parts.length > 0) ? parts[0] : null;
        String operationPart = (parts.length > 1) ? parts[1] : null;

        if (parts.length > 2) {
          regionNames = parts[2];
        }
        if (parts.length > 3) {
          keys = parts[3];
        }

        String regionPart = (regionNames != null) ? regionNames : "*";
        String keyPart = (keys != null) ? keys : "*";

        role.permissions.add(new ResourcePermission(resourcePart, operationPart, regionPart, keyPart));
      }

      roleMap.put(role.name, role);

      if (rolesNode.has("serverGroup")) {
        role.serverGroup = rolesNode.get("serverGroup").asText();
      }
    }

    return roleMap;
  }

  Collection<String> getUsers() {
    return userNameToUser.keySet();
  }

  public class Role {

    private List<ResourcePermission> permissions = new ArrayList<>();
    private String name;
    private String serverGroup;

    public String getName() {
      return name;
    }

    public void setName(final String name) {
      this.name = name;
    }

    public String getServerGroup() {
      return serverGroup;
    }

    public void setServerGroup(final String serverGroup) {
      this.serverGroup = serverGroup;
    }

    public List<ResourcePermission> getPermissions() {
      return permissions;
    }

    public void setPermissions(final List<ResourcePermission> permissions) {
      this.permissions = permissions;
    }
  }

  public class User {

    private String name;
    private Set<Role> roles = new HashSet<>();
    private String password;

    public String getName() {
      return name;
    }

    public void setName(final String name) {
      this.name = name;
    }

    public Set<Role> getRoles() {
      return roles;
    }

    public void setRoles(final Set<Role> roles) {
      this.roles = roles;
    }

    public String getPassword() {
      return password;
    }

    public void setPassword(final String password) {
      this.password = password;
    }
  }

}
