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
package org.apache.geode.management.internal.security;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertTrue;

import org.apache.shiro.authz.permission.WildcardPermission;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.security.ResourcePermission;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;
import org.apache.geode.security.ResourcePermission.Target;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({SecurityTest.class})
public class ResourcePermissionTest {
  @Test
  public void testEmptyConstructor() {
    ResourcePermission context = new ResourcePermission();
    assertThat(Resource.NULL).isEqualTo(context.getResource());
    assertThat(Operation.NULL).isEqualTo(context.getOperation());
    assertThat(ResourcePermission.ALL).isEqualTo(context.getTarget());
  }

  @Test
  public void testIsPermission() {
    ResourcePermission context = new ResourcePermission();
    assertTrue(context instanceof WildcardPermission);
  }

  @Test
  public void testConstructor() {
    ResourcePermission permission = new ResourcePermission();
    assertThat(Resource.NULL).isEqualTo(permission.getResource());
    assertThat(Operation.NULL).isEqualTo(permission.getOperation());
    assertThat(ResourcePermission.ALL).isEqualTo(permission.getTarget());

    permission = new ResourcePermission();
    assertThat(Resource.NULL).isEqualTo(permission.getResource());
    assertThat(Operation.NULL).isEqualTo(permission.getOperation());
    assertThat(ResourcePermission.ALL).isEqualTo(permission.getTarget());

    permission = new ResourcePermission(Resource.DATA, null);
    assertThat(Resource.DATA).isEqualTo(permission.getResource());
    assertThat(Operation.NULL).isEqualTo(permission.getOperation());
    assertThat(ResourcePermission.ALL).isEqualTo(permission.getTarget());

    permission = new ResourcePermission(Resource.CLUSTER, null);
    assertThat(Resource.CLUSTER).isEqualTo(permission.getResource());
    assertThat(Operation.NULL).isEqualTo(permission.getOperation());
    assertThat(ResourcePermission.ALL).isEqualTo(permission.getTarget());

    permission = new ResourcePermission(null, Operation.MANAGE, "REGIONA");
    assertThat(Resource.NULL).isEqualTo(permission.getResource());
    assertThat(Operation.MANAGE).isEqualTo(permission.getOperation());
    assertThat("REGIONA").isEqualTo(permission.getTarget());

    permission = new ResourcePermission(Resource.DATA, Operation.MANAGE, "REGIONA");
    assertThat(Resource.DATA).isEqualTo(permission.getResource());
    assertThat(Operation.MANAGE).isEqualTo(permission.getOperation());
    assertThat("REGIONA").isEqualTo(permission.getTarget());

    permission = new ResourcePermission(Resource.CLUSTER, Operation.MANAGE);
    assertThat(Resource.CLUSTER).isEqualTo(permission.getResource());
    assertThat(Operation.MANAGE).isEqualTo(permission.getOperation());
    assertThat(ResourcePermission.ALL).isEqualTo(permission.getTarget());

    // make sure "ALL" in the resource "DATA" means regionName won't be converted to *
    permission = new ResourcePermission(Resource.DATA, Operation.READ, "ALL");
    assertThat(Resource.DATA).isEqualTo(permission.getResource());
    assertThat(Operation.READ).isEqualTo(permission.getOperation());
    assertThat("ALL").isEqualTo(permission.getTarget());

    permission = new ResourcePermission(Resource.CLUSTER, Operation.READ, Target.ALL);
    assertThat(permission.getTarget()).isEqualTo(ResourcePermission.ALL);

    permission = new ResourcePermission(null, (String) null);
    assertThat(permission.getResourceString()).isEqualTo(ResourcePermission.NULL);
    assertThat(permission.getOperationString()).isEqualTo(ResourcePermission.NULL);
    assertThat(permission.getTarget()).isEqualTo(ResourcePermission.ALL);
    assertThat(permission.getKey()).isEqualTo(ResourcePermission.ALL);

    permission = new ResourcePermission(null, (Operation) null);
    assertThat(permission.getResource()).isEqualTo(Resource.NULL);
    assertThat(permission.getOperation()).isEqualTo(Operation.NULL);
    assertThat(permission.getResourceString()).isEqualTo(ResourcePermission.NULL);
    assertThat(permission.getOperationString()).isEqualTo(ResourcePermission.NULL);
    assertThat(permission.getTarget()).isEqualTo(ResourcePermission.ALL);
    assertThat(permission.getKey()).isEqualTo(ResourcePermission.ALL);

    permission = new ResourcePermission("*", "*");
    assertThat(permission.getResource()).isEqualTo(Resource.ALL);
    assertThat(permission.getOperation()).isEqualTo(Operation.ALL);
    assertThat(permission.getResourceString()).isEqualTo(ResourcePermission.ALL);
    assertThat(permission.getOperationString()).isEqualTo(ResourcePermission.ALL);
    assertThat(permission.getTarget()).isEqualTo(ResourcePermission.ALL);
    assertThat(permission.getKey()).isEqualTo(ResourcePermission.ALL);

    permission = new ResourcePermission("ALL", "ALL");
    assertThat(permission.getResource()).isEqualTo(Resource.ALL);
    assertThat(permission.getOperation()).isEqualTo(Operation.ALL);
    assertThat(permission.getResourceString()).isEqualTo(ResourcePermission.ALL);
    assertThat(permission.getOperationString()).isEqualTo(ResourcePermission.ALL);
    assertThat(permission.getTarget()).isEqualTo(ResourcePermission.ALL);
    assertThat(permission.getKey()).isEqualTo(ResourcePermission.ALL);

    permission = new ResourcePermission("NULL", "NULL");
    assertThat(permission.getResource()).isEqualTo(Resource.NULL);
    assertThat(permission.getOperation()).isEqualTo(Operation.NULL);
    assertThat(permission.getResourceString()).isEqualTo(ResourcePermission.NULL);
    assertThat(permission.getOperationString()).isEqualTo(ResourcePermission.NULL);
    assertThat(permission.getTarget()).isEqualTo(ResourcePermission.ALL);
    assertThat(permission.getKey()).isEqualTo(ResourcePermission.ALL);

    permission = new ResourcePermission("*", "READ");
    assertThat(permission.getResource()).isEqualTo(Resource.ALL);
    assertThat(permission.getOperation()).isEqualTo(Operation.READ);
    assertThat(permission.getResourceString()).isEqualTo(ResourcePermission.ALL);
    assertThat(permission.getOperationString()).isEqualTo("READ");
    assertThat(permission.getTarget()).isEqualTo(ResourcePermission.ALL);
    assertThat(permission.getKey()).isEqualTo(ResourcePermission.ALL);
    assertThat(permission.toString()).isEqualTo("*:READ");
  }

  @Test
  public void invalidResourceOperation() {
    assertThatThrownBy(() -> new ResourcePermission("invalid", "invalid"))
        .isInstanceOf(java.lang.IllegalArgumentException.class);
  }

  @Test
  public void regionNameIsStripped() {
    ResourcePermission permission = new ResourcePermission("DATA", "READ", SEPARATOR + "regionA");
    assertThat(permission.getResource()).isEqualTo(Resource.DATA);
    assertThat(permission.getOperation()).isEqualTo(Operation.READ);
    assertThat(permission.getTarget()).isEqualTo("regionA");
    assertThat(permission.getKey()).isEqualTo(ResourcePermission.ALL);
  }

  @Test
  public void allImplies() {
    ResourcePermission permission = ResourcePermissions.ALL;
    assertThat(permission.implies(new ResourcePermission("DATA", "READ"))).isTrue();
    assertThat(permission.implies(new ResourcePermission("DATA", "WRITE"))).isTrue();
    assertThat(permission.implies(new ResourcePermission("DATA", "MANAGE"))).isTrue();
    assertThat(permission.implies(new ResourcePermission("CLUSTER", "READ"))).isTrue();
    assertThat(permission.implies(new ResourcePermission("CLUSTER", "WRITE"))).isTrue();
    assertThat(permission.implies(new ResourcePermission("CLUSTER", "MANAGE"))).isTrue();

    permission = ResourcePermissions.DATA_ALL;
    assertThat(permission.implies(new ResourcePermission("DATA", "READ"))).isTrue();
    assertThat(permission.implies(new ResourcePermission("DATA", "WRITE"))).isTrue();
    assertThat(permission.implies(new ResourcePermission("DATA", "MANAGE"))).isTrue();
    assertThat(permission.implies(new ResourcePermission("CLUSTER", "READ"))).isFalse();
    assertThat(permission.implies(new ResourcePermission("CLUSTER", "WRITE"))).isFalse();
    assertThat(permission.implies(new ResourcePermission("CLUSTER", "MANAGE"))).isFalse();

    permission = ResourcePermissions.CLUSTER_ALL;
    assertThat(permission.implies(new ResourcePermission("DATA", "READ"))).isFalse();
    assertThat(permission.implies(new ResourcePermission("DATA", "WRITE"))).isFalse();
    assertThat(permission.implies(new ResourcePermission("DATA", "MANAGE"))).isFalse();
    assertThat(permission.implies(new ResourcePermission("CLUSTER", "READ"))).isTrue();
    assertThat(permission.implies(new ResourcePermission("CLUSTER", "WRITE"))).isTrue();
    assertThat(permission.implies(new ResourcePermission("CLUSTER", "MANAGE"))).isTrue();
  }

  @Test
  public void testToString() {
    ResourcePermission context = new ResourcePermission();
    assertThat("NULL:NULL").isEqualTo(context.toString());

    context = new ResourcePermission("data", "manage");
    assertThat("DATA:MANAGE").isEqualTo(context.toString());

    context = new ResourcePermission("data", "read", "regionA");
    assertThat("DATA:READ:regionA").isEqualTo(context.toString());

    context = new ResourcePermission("DATA", "READ", SEPARATOR + "regionA", "key");
    assertThat("DATA:READ:regionA:key").isEqualTo(context.toString());

    context = new ResourcePermission(Resource.DATA, Operation.MANAGE, "REGIONA");
    assertThat("DATA:MANAGE:REGIONA").isEqualTo(context.toString());

    context = new ResourcePermission(Resource.DATA, Operation.MANAGE);
    assertThat("DATA:MANAGE").isEqualTo(context.toString());

    context = new ResourcePermission("ALL", "READ");
    assertThat(context.toString()).isEqualTo("*:READ");

    context = new ResourcePermission("DATA", "ALL");
    assertThat(context.toString()).isEqualTo("DATA");

    context = new ResourcePermission("ALL", "ALL", "regionA", "*");
    assertThat(context.toString()).isEqualTo("*:*:regionA");
  }

  @Test
  public void impliesWithWildCardPermission() {
    // If caseSensitive=false, the permission string becomes lower-case, which will cause failures
    // when testing implication against our (case sensitive) resources, e.g., DATA

    WildcardPermission context = new WildcardPermission("*:READ", true);
    assertThat(context.implies(new ResourcePermission(Resource.DATA, Operation.READ))).isTrue();
    assertThat(context.implies(new ResourcePermission(Resource.CLUSTER, Operation.READ))).isTrue();

    context = new WildcardPermission("*:READ:*", true);
    assertThat(context.implies(new ResourcePermission(Resource.DATA, Operation.READ, "testRegion")))
        .isTrue();
    assertThat(context
        .implies(new ResourcePermission(Resource.CLUSTER, Operation.READ, "anotherRegion", "key1")))
            .isTrue();

    context = new WildcardPermission("DATA:*:testRegion", true);
    assertThat(context.implies(new ResourcePermission(Resource.DATA, Operation.READ, "testRegion")))
        .isTrue();
    assertThat(
        context.implies(new ResourcePermission(Resource.DATA, Operation.WRITE, "testRegion")))
            .isTrue();
  }
}
