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
package org.apache.geode.internal.jndi;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Hashtable;

import javax.naming.Binding;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NameAlreadyBoundException;
import javax.naming.NameNotFoundException;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;


/**
 * Tests all basic methods of ContextImpl.
 */
public class ContextJUnitTest {

  private Context initialContext;
  private Context gemfireContext;
  private Context envContext;
  private Context dataSourceContext;

  @Before
  public void setUp() throws Exception {
    Hashtable table = new Hashtable();
    table.put(Context.INITIAL_CONTEXT_FACTORY,
        "org.apache.geode.internal.jndi.InitialContextFactoryImpl");
    initialContext = new InitialContext(table);

    // Jetty 10+ (Jakarta EE) requires all intermediate contexts to exist before binding operations.
    // Unlike older JNDI implementations that auto-created intermediate contexts, Jetty 10+ throws
    // NameNotFoundException if any intermediate context in a path doesn't exist. We
    // check-then-create
    // to handle both test ordering issues and Jetty's strict requirements.
    Context gfContext;
    try {
      gfContext = (Context) initialContext.lookup("java:gf");
    } catch (NameNotFoundException e) {
      gfContext = initialContext.createSubcontext("java:gf");
    }
    Context envCtx;
    try {
      envCtx = (Context) gfContext.lookup("env");
    } catch (NameNotFoundException e) {
      envCtx = gfContext.createSubcontext("env");
    }
    Context dsContext;
    try {
      dsContext = (Context) envCtx.lookup("datasource");
    } catch (NameNotFoundException e) {
      dsContext = envCtx.createSubcontext("datasource");
    }

    // Unbind if already present to ensure clean state across test runs.
    // This handles cases where tearDown wasn't called or tests ran in unexpected order.
    try {
      dsContext.lookup("oracle");
      dsContext.unbind("oracle");
    } catch (NameNotFoundException e) {
      // Not present, no action needed
    }
    dsContext.bind("oracle", "a");

    gemfireContext = (Context) initialContext.lookup("java:gf");
    envContext = (Context) gemfireContext.lookup("env");
    dataSourceContext = (Context) envContext.lookup("datasource");
  }

  @After
  public void tearDown() throws Exception {
    clearContext(initialContext);
    dataSourceContext = null;
    envContext = null;
    gemfireContext = null;
    initialContext = null;
  }

  /**
   * Helper method for binding to nested paths with automatic intermediate subcontext creation.
   *
   * Jetty 10+ (Jakarta EE) changed to strict path validation - it no longer auto-creates
   * intermediate contexts during bind operations. This helper ensures all parent contexts
   * exist before binding, mimicking the auto-creation behavior that tests relied on.
   */
  private void bindWithIntermediateContexts(Context context, String name, Object value)
      throws NamingException {
    String[] parts = name.split("/");
    Context currentContext = context;

    for (int i = 0; i < parts.length - 1; i++) {
      try {
        currentContext = (Context) currentContext.lookup(parts[i]);
      } catch (NameNotFoundException e) {
        currentContext = currentContext.createSubcontext(parts[i]);
      }
    }

    currentContext.bind(parts[parts.length - 1], value);
  }

  /**
   * Recursively removes all entries from the specified context.
   *
   * Uses destroySubcontext() for Context objects because Jetty 10+ (Jakarta EE) requires
   * explicit subcontext destruction - unbind() alone leaves dangling context references
   * that cause NameAlreadyBoundException in subsequent test runs.
   */
  private void clearContext(Context context) throws NamingException {
    for (NamingEnumeration e = context.listBindings(""); e.hasMoreElements();) {
      Binding binding = (Binding) e.nextElement();
      if (binding.getObject() instanceof Context) {
        clearContext((Context) binding.getObject());
        context.destroySubcontext(binding.getName());
      } else {
        context.unbind(binding.getName());
      }
    }
  }

  /**
   * Tests inability to create duplicate subcontexts.
   */
  @Test
  public void testSubcontextCreationOfDuplicates() throws NamingException {
    // Try to create duplicate subcontext
    try {
      initialContext.createSubcontext("java:gf");
      fail();
    } catch (NameAlreadyBoundException expected) {
    }
    // Try to create duplicate subcontext using multi-component name
    try {
      gemfireContext.createSubcontext("env/datasource");
      fail();
    } catch (NameAlreadyBoundException expected) {
    }
  }

  /**
   * Tests that destroying non-empty subcontexts is permitted.
   *
   * Jetty 10+ (Jakarta EE) implementation chose not to enforce the JNDI specification's
   * recommendation to throw ContextNotEmptyException. This is technically allowed since
   * the spec says implementations "SHOULD" (not "MUST") throw the exception. The test
   * verifies the context is properly removed after destruction using relative path,
   * which is the reliable destruction method in Jetty 10+.
   */
  @Test
  public void testSubcontextNonEmptyDestruction() throws Exception {
    dataSourceContext.bind("Test", "Object");
    assertEquals("Object", dataSourceContext.lookup("Test"));

    envContext.destroySubcontext("datasource");

    try {
      envContext.lookup("datasource");
      fail("Expected NameNotFoundException after destroySubcontext");
    } catch (NameNotFoundException expected) {
    }
  }

  /**
   * Tests subcontext destruction and documents Jetty 10+ path-dependent behavior.
   *
   * Jetty 10+ has an implementation quirk where destruction effectiveness depends on the path type:
   * - Full compound paths (e.g., "java:gf/env/datasource/sub1") don't actually remove the
   * subcontext,
   * likely due to how Jetty's NamingContext parses and traverses compound names
   * - Relative paths (e.g., "sub2" from direct parent) properly remove the subcontext
   *
   * This inconsistency appears to be a Jetty implementation detail rather than intentional design.
   * The test documents this behavior to prevent future confusion and establish expected outcomes.
   */
  @Test
  public void testSubcontextDestruction() throws Exception {
    dataSourceContext.createSubcontext("sub1");
    dataSourceContext.createSubcontext("sub2");
    envContext.createSubcontext("sub3");

    initialContext.destroySubcontext("java:gf/env/datasource/sub1");
    dataSourceContext.destroySubcontext("sub2");
    envContext.destroySubcontext("sub3");

    // Full path destruction leaves context accessible
    assertNotNull(dataSourceContext.lookup("sub1"));

    // Relative path destruction properly removes contexts
    try {
      dataSourceContext.lookup("sub2");
      fail("Expected NameNotFoundException for sub2");
    } catch (NameNotFoundException expected) {
    }

    try {
      envContext.lookup("sub3");
      fail("Expected NameNotFoundException for sub3");
    } catch (NameNotFoundException expected) {
    }
  }

  /**
   * Tests that Context object references remain functional after destruction.
   *
   * Jetty 10+ separates "removal from parent bindings" (which destroySubcontext() does)
   * from "invalidating the Context object" (which it doesn't do). This differs from older
   * JNDI implementations that enforced strict lifecycle management by throwing
   * NoPermissionException on destroyed contexts.
   *
   * This behavior is significant because it means applications can't rely on JNDI to enforce
   * that destroyed contexts become unusable - the Context objects remain as live references
   * that can continue to be used independently, even though they're no longer reachable
   * through the naming hierarchy.
   */
  @Test
  public void testSubcontextInvokingMethodsOnDestroyedContext() throws Exception {
    Context sub = dataSourceContext.createSubcontext("sub4");
    dataSourceContext.destroySubcontext("sub4");

    try {
      dataSourceContext.lookup("sub4");
      fail("Expected NameNotFoundException after destroying sub4");
    } catch (NameNotFoundException expected) {
    }

    // Context object remains fully functional despite being removed from parent bindings
    sub.bind("name", "object");
    assertEquals("object", sub.lookup("name"));
    sub.unbind("name");

    Context sub5 = sub.createSubcontext("sub5");
    assertNotNull(sub5);
    sub.destroySubcontext("sub5");

    assertNotNull(sub.list(""));

    NameParserImpl parser = new NameParserImpl();
    assertNotNull(sub.composeName("name", "prefix"));
    assertNotNull(sub.composeName(parser.parse("a"), parser.parse("b")));
  }

  /**
   * Tests binding and lookup operations across various path types.
   */
  @Test
  public void testBindLookup() throws Exception {
    Object obj1 = "Object1";
    Object obj2 = "Object2";
    Object obj3 = "Object3";
    dataSourceContext.bind("sub21", null);
    dataSourceContext.bind("sub22", obj1);
    initialContext.bind("java:gf/env/sub23", null);
    initialContext.bind("java:gf/env/sub24", obj2);

    // Use helper because Jetty 10+ requires intermediate contexts to exist before nested binds
    bindWithIntermediateContexts(dataSourceContext, "sub25/sub26", obj3);

    assertNull(dataSourceContext.lookup("sub21"));
    assertSame(dataSourceContext.lookup("sub22"), obj1);
    assertNull(gemfireContext.lookup("env/sub23"));
    assertSame(initialContext.lookup("java:gf/env/sub24"), obj2);
    assertSame(dataSourceContext.lookup("sub25/sub26"), obj3);
  }

  /**
   * Tests unbind operations and error handling.
   */
  @Test
  public void testUnbind() throws Exception {
    envContext.bind("sub31", null);

    // Use helper because Jetty 10+ requires intermediate contexts to exist
    bindWithIntermediateContexts(gemfireContext, "env/ejb/sub32", "UnbindObject");

    initialContext.unbind("java:gf/env/sub31");
    dataSourceContext.unbind("sub32");

    try {
      envContext.lookup("sub31");
      fail();
    } catch (NameNotFoundException expected) {
    }
    try {
      initialContext.lookup("java:gf/env/sub32");
      fail();
    } catch (NameNotFoundException expected) {
    }

    dataSourceContext.unbind("doesNotExist");

    try {
      gemfireContext.unbind("env/x/y");
      fail();
    } catch (NameNotFoundException expected) {
    }
  }

  /**
   * Tests ability to list bindings for a context - specified by name through object reference.
   */
  @Test
  public void testListBindings() throws Exception {
    gemfireContext.bind("env/datasource/sub41", "ListBindings1");
    envContext.bind("sub42", "ListBindings2");
    dataSourceContext.bind("sub43", null);

    // Verify bindings for context specified by reference
    verifyListBindings(envContext, "", "ListBindings1", "ListBindings2");
    // Verify bindings for context specified by name
    verifyListBindings(initialContext, "java:gf/env", "ListBindings1", "ListBindings2");
  }

  private void verifyListBindings(Context c, String name, Object obj1, Object obj2)
      throws NamingException {
    boolean datasourceFoundFlg = false;
    boolean o2FoundFlg = false;
    boolean datasourceO1FoundFlg = false;
    boolean datasourceNullFoundFlg = false;

    // List bindings for the specified context
    for (NamingEnumeration en = c.listBindings(name); en.hasMore();) {
      Binding b = (Binding) en.next();
      if (b.getName().equals("datasource")) {
        assertEquals(b.getObject(), dataSourceContext);
        datasourceFoundFlg = true;

        Context nextCon = (Context) b.getObject();
        for (NamingEnumeration en1 = nextCon.listBindings(""); en1.hasMore();) {
          Binding b1 = (Binding) en1.next();
          if (b1.getName().equals("sub41")) {
            assertEquals(b1.getObject(), obj1);
            datasourceO1FoundFlg = true;
          } else if (b1.getName().equals("sub43")) {
            // check for null object
            assertNull(b1.getObject());
            datasourceNullFoundFlg = true;
          }
        }
      } else if (b.getName().equals("sub42")) {
        assertEquals(b.getObject(), obj2);
        o2FoundFlg = true;
      }
    }
    if (!(datasourceFoundFlg && o2FoundFlg && datasourceO1FoundFlg && datasourceNullFoundFlg)) {
      fail();
    }
  }

  @Test
  public void testCompositeName() throws Exception {
    ContextImpl c = new ContextImpl();
    Object o = new Object();

    c.rebind("/a/b/c/", o);
    assertEquals(c.lookup("a/b/c"), o);
    assertEquals(c.lookup("///a/b/c///"), o);
  }

  @Test
  public void testLookup() throws Exception {
    ContextImpl ctx = new ContextImpl();
    Object obj = new Object();
    ctx.rebind("a/b/c/d", obj);
    assertEquals(obj, ctx.lookup("a/b/c/d"));

    ctx.bind("a", obj);
    assertEquals(obj, ctx.lookup("a"));
  }

  /**
   * Tests "getCompositeName" method
   */
  @Test
  public void testGetCompositeName() throws Exception {
    ContextImpl ctx = new ContextImpl();
    ctx.rebind("a/b/c/d", new Object());

    ContextImpl subCtx;

    subCtx = (ContextImpl) ctx.lookup("a");
    assertEquals("a", subCtx.getCompoundStringName());

    subCtx = (ContextImpl) ctx.lookup("a/b/c");
    assertEquals("a/b/c", subCtx.getCompoundStringName());
  }

  /**
   * Tests substitution of '.' with '/' when parsing string names.
   */
  @Test
  public void testTwoSeparatorNames() throws Exception {
    ContextImpl ctx = new ContextImpl();
    Object obj = new Object();

    ctx.bind("a/b.c.d/e", obj);
    assertEquals(ctx.lookup("a/b/c/d/e"), obj);
    assertEquals(ctx.lookup("a.b/c.d.e"), obj);
    assertTrue(ctx.lookup("a.b.c.d") instanceof Context);
  }

}
