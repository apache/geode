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
package com.gemstone.gemfire.internal.jndi;

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

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.junit.categories.UnitTest;

import junit.framework.TestCase;

//import com.gemstone.gemfire.internal.jndi.InitialContextFactoryImpl;

/**
 * Tests all basic methods of ContextImpl.
 * 
 * @author Nand Kishor Jha
 */
@Category(UnitTest.class)
public class ContextJUnitTest extends TestCase {
  
  private Context initialCtx;
  
  private Context gfCtx;
  
  private Context envCtx;
  
  private Context datasourceCtx;
  
  public ContextJUnitTest(String name) {
    super(name);
  }
  
  protected void setUp() throws Exception {
    // InitialContextFactoryImpl impl = new InitialContextFactoryImpl();
    //	impl.setAsInitial();
    Hashtable table = new Hashtable();
    table
    .put(
        Context.INITIAL_CONTEXT_FACTORY,
    "com.gemstone.gemfire.internal.jndi.InitialContextFactoryImpl");
    //	table.put(Context.URL_PKG_PREFIXES,
    // "com.gemstone.gemfire.internal.jndi");
    initialCtx = new InitialContext(table);
    initialCtx.bind("java:gf/env/datasource/oracle", "a");
    gfCtx = (Context) initialCtx.lookup("java:gf");
    envCtx = (Context) gfCtx.lookup("env");
    datasourceCtx = (Context) envCtx.lookup("datasource");
  }
  
  protected void tearDown() throws Exception {
    
    clearContext(initialCtx);
    datasourceCtx = null;
    envCtx = null;
    gfCtx = null;
    initialCtx = null;
    //InitialContextFactoryImpl.revertSetAsInitial();
  }
  
  /**
   * Removes all entries from the specified context, including subcontexts.
   * 
   * @param context context ot clear
   */
  private void clearContext(Context context)
  throws NamingException {
    
    for (NamingEnumeration e = context.listBindings(""); e
    .hasMoreElements();) {
      Binding binding = (Binding) e.nextElement();
      if (binding.getObject() instanceof Context) {
        clearContext((Context) binding.getObject());
      }
      context.unbind(binding.getName());
    }
    
  }
  
  /*
   * Tests inability to create duplicate subcontexts.
   * 
   * @throws NamingException
   */
  public void testSubcontextCreationOfDuplicates()
  throws NamingException {
    
    // Try to create duplicate subcontext
    try {
      initialCtx.createSubcontext("java:gf");
      fail();
    }
    catch (NameAlreadyBoundException ex) {
    }
    // Try to create duplicate subcontext using multi-component name
    try {
      gfCtx.createSubcontext("env/datasource");
      fail();
    }
    catch (NameAlreadyBoundException ex) {
    }
  }
  
  /*
   * Tests inability to destroy non empty subcontexts.
   * 
   * @throws NamingException
   */
  public void testSubcontextNonEmptyDestruction()
  throws NamingException {
    
    // Bind some object in ejb subcontext
    datasourceCtx.bind("Test", "Object");
    // Attempt to destroy any subcontext
    try {
      initialCtx.destroySubcontext("java:gf");
      fail();
    }
    catch (ContextNotEmptyException ex) {
    }
    try {
      initialCtx
      .destroySubcontext("java:gf/env/datasource");
      fail();
    }
    catch (ContextNotEmptyException ex) {
    }
    try {
      envCtx.destroySubcontext("datasource");
      fail();
    }
    catch (ContextNotEmptyException ex) {
    }
  }
  
  /*
   * Tests ability to destroy empty subcontexts.
   * 
   * @throws NamingException
   */
  public void testSubcontextDestruction()
  throws NamingException {
    
    // Create three new subcontexts
    datasourceCtx.createSubcontext("sub1");
    datasourceCtx.createSubcontext("sub2");
    envCtx.createSubcontext("sub3");
    // Destroy
    initialCtx
    .destroySubcontext("java:gf/env/datasource/sub1");
    datasourceCtx.destroySubcontext("sub2");
    envCtx.destroySubcontext("sub3");
    // Perform lookup
    try {
      datasourceCtx.lookup("sub1");
      fail();
    }
    catch (NameNotFoundException ex) {
    }
    try {
      envCtx.lookup("datasource/sub2");
      fail();
    }
    catch (NameNotFoundException ex) {
    }
    try {
      initialCtx.lookup("java:gf/sub3");
      fail();
    }
    catch (NameNotFoundException ex) {
    }
  }
  
  /*
   * Tests inability to invoke methods on destroyed subcontexts. @throws
   * NamingException
   */
  public void testSubcontextInvokingMethodsOnDestroyedContext()
  throws NamingException {
    
    //Create subcontext and destroy it.
    Context sub = datasourceCtx.createSubcontext("sub4");
    initialCtx
    .destroySubcontext("java:gf/env/datasource/sub4");
    
    try {
      sub.bind("name", "object");
      fail();
    }
    catch (NoPermissionException ex) {
    }
    try {
      sub.unbind("name");
      fail();
    }
    catch (NoPermissionException ex) {
    }
    try {
      sub.createSubcontext("sub5");
      fail();
    }
    catch (NoPermissionException ex) {
    }
    try {
      sub.destroySubcontext("sub6");
      fail();
    }
    catch (NoPermissionException ex) {
    }
    try {
      sub.list("");
      fail();
    }
    catch (NoPermissionException ex) {
    }
    try {
      sub.lookup("name");
      fail();
    }
    catch (NoPermissionException ex) {
    }
    try {
      sub.composeName("name", "prefix");
      fail();
    }
    catch (NoPermissionException ex) {
    }
    try {
      NameParserImpl parser = new NameParserImpl();
      sub.composeName(parser.parse("a"), parser.parse("b"));
      fail();
    }
    catch (NoPermissionException ex) {
    }
  }
  
  /*
   * Tests ability to bind name to object. @throws NamingException
   */
  public void testBindLookup() throws NamingException {
    
    Object obj1 = new String("Object1");
    Object obj2 = new String("Object2");
    Object obj3 = new String("Object3");
    datasourceCtx.bind("sub21", null);
    datasourceCtx.bind("sub22", obj1);
    initialCtx.bind("java:gf/env/sub23", null);
    initialCtx.bind("java:gf/env/sub24", obj2);
    // Bind to subcontexts that do not exist
    initialCtx.bind("java:gf/env/datasource/sub25/sub26",
        obj3);
    
    // Try to lookup
    assertNull(datasourceCtx.lookup("sub21"));
    assertSame(datasourceCtx.lookup("sub22"), obj1);
    assertNull(gfCtx.lookup("env/sub23"));
    assertSame(initialCtx.lookup("java:gf/env/sub24"), obj2);
    assertSame(datasourceCtx.lookup("sub25/sub26"), obj3);
  }
  
  /*
   * Tests ability to unbind names. @throws NamingException
   */
  public void testUnbind() throws NamingException {
    
    envCtx.bind("sub31", null);
    gfCtx.bind("env/ejb/sub32", new String("UnbindObject"));
    // Unbind
    initialCtx.unbind("java:gf/env/sub31");
    datasourceCtx.unbind("sub32");
    try {
      envCtx.lookup("sub31");
      fail();
    }
    catch (NameNotFoundException ex) {
    }
    try {
      initialCtx.lookup("java:gf/env/sub32");
      fail();
    }
    catch (NameNotFoundException ex) {
    }
    // Unbind non-existing name
    try {
      datasourceCtx.unbind("doesNotExist");
    }
    catch (Exception ex) {
      fail();
    }
    // Unbind non-existing name, when subcontext does not exists
    try {
      gfCtx.unbind("env/x/y");
      fail();
    }
    catch (NameNotFoundException ex) {
    }
  }
  
  /*
   * Tests ability to list bindings for a context - specified by name through
   * object reference.
   * 
   * @throws NamingException
   */
  public void testListBindings() throws NamingException {
    
    gfCtx.bind("env/datasource/sub41", "ListBindings1");
    envCtx.bind("sub42", "ListBindings2");
    datasourceCtx.bind("sub43", null);
    
    // Verify bindings for context specified by reference
    verifyListBindings(envCtx, "", "ListBindings1",
    "ListBindings2");
    // Verify bindings for context specified by name
    verifyListBindings(initialCtx, "java:gf/env",
        "ListBindings1", "ListBindings2");
  }
  
  private void verifyListBindings(Context c, String name,
      Object obj1, Object obj2) throws NamingException {
    
    boolean datasourceFoundFlg = false;
    boolean o2FoundFlg = false;
    boolean datasourceO1FoundFlg = false;
    boolean datasourceNullFoundFlg = false;
    
    // List bindings for the specified context
    for (NamingEnumeration en = c.listBindings(name); en
    .hasMore();) {
      Binding b = (Binding) en.next();
      if (b.getName().equals("datasource")) {
        assertEquals(b.getObject(), datasourceCtx);
        datasourceFoundFlg = true;
        
        Context nextCon = (Context) b.getObject();
        for (NamingEnumeration en1 = nextCon
            .listBindings(""); en1.hasMore();) {
          Binding b1 = (Binding) en1.next();
          if (b1.getName().equals("sub41")) {
            assertEquals(b1.getObject(), obj1);
            datasourceO1FoundFlg = true;
          }
          else if (b1.getName().equals("sub43")) {
            // check for null object
            assertNull(b1.getObject());
            datasourceNullFoundFlg = true;
          }
        }
      }
      else if (b.getName().equals("sub42")) {
        assertEquals(b.getObject(), obj2);
        o2FoundFlg = true;
      }
    }
    if (!(datasourceFoundFlg && o2FoundFlg
        && datasourceO1FoundFlg && datasourceNullFoundFlg)) {
      fail();
    }
  }
  
  public void testCompositeName() throws Exception {
    ContextImpl c = new ContextImpl();
    Object o = new Object();
    
    c.rebind("/a/b/c/", o);
    assertEquals(c.lookup("a/b/c"), o);
    assertEquals(c.lookup("///a/b/c///"), o);
    
  }
  
  public void testLookup() throws Exception {
    ContextImpl ctx = new ContextImpl();
    Object obj = new Object();
    ctx.rebind("a/b/c/d", obj);
    assertEquals(obj, ctx.lookup("a/b/c/d"));
    
    ctx.bind("a", obj);
    assertEquals(obj, ctx.lookup("a"));
    
  }
  
  /*
   * Tests "getCompositeName" method
   */
  public void testGetCompositeName() throws Exception {
    
    ContextImpl ctx = new ContextImpl();
    ctx.rebind("a/b/c/d", new Object());
    
    ContextImpl subCtx;
    
    subCtx = (ContextImpl) ctx.lookup("a");
    assertEquals("a", subCtx.getCompoundStringName());
    
    subCtx = (ContextImpl) ctx.lookup("a/b/c");
    assertEquals("a/b/c", subCtx.getCompoundStringName());
    
  }
  
  /*
   * Tests substitution of '.' with '/' when parsing string names. @throws
   * NamingException
   */
  public void testTwoSeparatorNames()
  throws NamingException {
    ContextImpl ctx = new ContextImpl();
    Object obj = new Object();
    
    ctx.bind("a/b.c.d/e", obj);
    assertEquals(ctx.lookup("a/b/c/d/e"), obj);
    assertEquals(ctx.lookup("a.b/c.d.e"), obj);
    assertTrue(ctx.lookup("a.b.c.d") instanceof Context);
  }
  
}