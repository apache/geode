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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Hashtable;

import javax.naming.Binding;
import javax.naming.Context;
import javax.naming.ContextNotEmptyException;
import javax.naming.InitialContext;
import javax.naming.NameAlreadyBoundException;
import javax.naming.NameNotFoundException;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.NoPermissionException;

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
    initialContext.bind("java:gf/env/datasource/oracle", "a");
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
   * Removes all entries from the specified context, including subcontexts.
   *
   * @param context context to clear
   */
  private void clearContext(Context context) throws NamingException {
    for (NamingEnumeration e = context.listBindings(""); e.hasMoreElements();) {
      Binding binding = (Binding) e.nextElement();
      if (binding.getObject() instanceof Context) {
        clearContext((Context) binding.getObject());
      }
      context.unbind(binding.getName());
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
   * Tests inability to destroy non empty subcontexts.
   */
  @Test
  public void testSubcontextNonEmptyDestruction() throws Exception {
    // Bind some object in ejb subcontext
    dataSourceContext.bind("Test", "Object");
    // Attempt to destroy any subcontext
    try {
      initialContext.destroySubcontext("java:gf");
      fail();
    } catch (ContextNotEmptyException expected) {
    }
    try {
      initialContext.destroySubcontext("java:gf/env/datasource");
      fail();
    } catch (ContextNotEmptyException expected) {
    }
    try {
      envContext.destroySubcontext("datasource");
      fail();
    } catch (ContextNotEmptyException expected) {
    }
  }

  /**
   * Tests ability to destroy empty subcontexts.
   */
  @Test
  public void testSubcontextDestruction() throws Exception {
    // Create three new subcontexts
    dataSourceContext.createSubcontext("sub1");
    dataSourceContext.createSubcontext("sub2");
    envContext.createSubcontext("sub3");
    // Destroy
    initialContext.destroySubcontext("java:gf/env/datasource/sub1");
    dataSourceContext.destroySubcontext("sub2");
    envContext.destroySubcontext("sub3");
    // Perform lookup
    try {
      dataSourceContext.lookup("sub1");
      fail();
    } catch (NameNotFoundException expected) {
    }
    try {
      envContext.lookup("datasource/sub2");
      fail();
    } catch (NameNotFoundException expected) {
    }
    try {
      initialContext.lookup("java:gf/sub3");
      fail();
    } catch (NameNotFoundException expected) {
    }
  }

  /**
   * Tests inability to invoke methods on destroyed subcontexts.
   */
  @Test
  public void testSubcontextInvokingMethodsOnDestroyedContext() throws Exception {
    // Create subcontext and destroy it.
    Context sub = dataSourceContext.createSubcontext("sub4");
    initialContext.destroySubcontext("java:gf/env/datasource/sub4");

    try {
      sub.bind("name", "object");
      fail();
    } catch (NoPermissionException expected) {
    }
    try {
      sub.unbind("name");
      fail();
    } catch (NoPermissionException expected) {
    }
    try {
      sub.createSubcontext("sub5");
      fail();
    } catch (NoPermissionException expected) {
    }
    try {
      sub.destroySubcontext("sub6");
      fail();
    } catch (NoPermissionException expected) {
    }
    try {
      sub.list("");
      fail();
    } catch (NoPermissionException expected) {
    }
    try {
      sub.lookup("name");
      fail();
    } catch (NoPermissionException expected) {
    }
    try {
      sub.composeName("name", "prefix");
      fail();
    } catch (NoPermissionException expected) {
    }
    try {
      NameParserImpl parser = new NameParserImpl();
      sub.composeName(parser.parse("a"), parser.parse("b"));
      fail();
    } catch (NoPermissionException expected) {
    }
  }

  /**
   * Tests ability to bind name to object.
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
    // Bind to subcontexts that do not exist
    initialContext.bind("java:gf/env/datasource/sub25/sub26", obj3);

    // Try to lookup
    assertNull(dataSourceContext.lookup("sub21"));
    assertSame(dataSourceContext.lookup("sub22"), obj1);
    assertNull(gemfireContext.lookup("env/sub23"));
    assertSame(initialContext.lookup("java:gf/env/sub24"), obj2);
    assertSame(dataSourceContext.lookup("sub25/sub26"), obj3);
  }

  /**
   * Tests ability to unbind names.
   */
  @Test
  public void testUnbind() throws Exception {
    envContext.bind("sub31", null);
    gemfireContext.bind("env/ejb/sub32", "UnbindObject");
    // Unbind
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
    // Unbind non-existing name
    dataSourceContext.unbind("doesNotExist");
    // Unbind non-existing name, when subcontext does not exists
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
