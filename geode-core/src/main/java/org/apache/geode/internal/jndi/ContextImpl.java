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

import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Vector;

import javax.naming.Binding;
import javax.naming.CompositeName;
import javax.naming.Context;
import javax.naming.ContextNotEmptyException;
import javax.naming.InvalidNameException;
import javax.naming.Name;
import javax.naming.NameAlreadyBoundException;
import javax.naming.NameNotFoundException;
import javax.naming.NameParser;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.NoPermissionException;
import javax.naming.NotContextException;
import javax.transaction.SystemException;

import org.apache.geode.LogWriter;
import org.apache.geode.internal.jta.TransactionUtils;
import org.apache.geode.internal.jta.UserTransactionImpl;

/**
 * Provides implementation of javax.naming.Context interface. A name in the ContextImpl namespace is
 * a sequence of one or more atomic names, relative to a root initial context. When a name consist
 * of more than one atomic names it is a CompoundName where atomic names are separated with
 * separator character - '/' or '.'. It is possible to use both separator characters in the same
 * name. In such cases any occurrences of '.' are replaced with '/' before parsing. This rule can be
 * altered/modified by making changes in NameParserImpl class.
 *
 */
public class ContextImpl implements Context {

  private static final String ROOT_CONTEXT_NAME = "ROOT";
  // Naming scheme for the ContextImpl.
  private static final NameParser nameParser = new NameParserImpl();
  /*
   * Map of objects registered for this context representing the local context
   */
  private final Map ctxMaps = Collections.synchronizedMap(new HashMap());
  // Name of this Context
  private String ctxName;
  // Parent Context of this Context
  private ContextImpl parentCtx;
  // Shows if this context has been destroyed
  private boolean isDestroyed;

  /*
   * Creates new instance of ContextImpl. @param parentCtx parent context of this context. null if
   * this is the root context. @param name atomic name for this context
   */
  private ContextImpl(ContextImpl parentCtx, String name) {
    this.parentCtx = parentCtx;
    this.ctxName = name;
    this.isDestroyed = false;
  }

  /**
   * Default constructor
   */
  public ContextImpl() {
    // call the constructor with default setting
    this(null, ROOT_CONTEXT_NAME);
  }

  /**
   * Not implemented
   *
   */
  public Object addToEnvironment(String key, Object value) throws NamingException {
    throw new NamingException(
        "addToEnvironment(String key, Object value) is not implemented");
  }

  /**
   * Binds object to a name in this context. Intermediate contexts that do not exist will be
   * created.
   *
   * @param name Name of the object to bind
   * @param obj Object to bind. Can be null.
   * @throws NoPermissionException if this context has been destroyed.
   * @throws InvalidNameException if name is empty or is CompositeName that spans more than one
   *         naming system.
   * @throws NotContextException if name has more than one atomic name and intermediate atomic name
   *         is bound to object that is not context.
   */
  public void bind(Name name, Object obj) throws NamingException {
    checkIsDestroyed();
    // Do not check for already bound name.
    // Simply replace the existing value.
    rebind(name, obj);
  }

  /**
   * Binds object to name in this context.
   *
   * @param name * name of the object to add
   * @param obj object to bind
   * @throws NamingException if naming error occurs
   *
   */
  public void bind(String name, Object obj) throws NamingException {
    bind(nameParser.parse(name), obj);
  }

  /**
   * Does nothing.
   */
  public void close() throws NamingException {}

  /**
   * Returns composition of prefix and name .
   *
   * @param name name relative to this context
   * @param prefix name of this context Example: composeName("a","b") : b/a composeName("a","") : a
   *
   */
  public Name composeName(Name name, Name prefix) throws NamingException {
    checkIsDestroyed();
    // We do not want to modify any of the parameters (JNDI requirement).
    // Clone <code> prefix </code> to satisfy the requirement.
    Name parsedPrefix = getParsedName((Name) prefix.clone());
    Name parsedName = getParsedName(name);
    return parsedPrefix.addAll(parsedName);
  }

  /**
   * Returns composition of prefix and name .
   *
   * @param name name relative to this context
   * @param prefix name of this context Example: composeName("a","b") : b/a composeName("a","") : a
   *
   */
  public String composeName(String name, String prefix) throws NamingException {
    checkIsDestroyed();
    return composeName(nameParser.parse(name), nameParser.parse(prefix)).toString();
  }

  /**
   * Creates subcontext with name, relative to this Context.
   *
   * @param name subcontext name.
   * @return new subcontext named name relative to this context.
   * @throws NoPermissionException if this context has been destroyed.
   * @throws InvalidNameException if name is empty or is CompositeName that spans more than one
   *         naming system.
   * @throws NameAlreadyBoundException if name is already bound in this Context
   * @throws NotContextException if any intermediate name from name is not bound to instance of
   *         javax.naming.Context.
   *
   */
  public Context createSubcontext(Name name) throws NamingException {
    checkIsDestroyed();
    Name parsedName = getParsedName(name);
    if (parsedName.size() == 0 || parsedName.get(0).length() == 0) {
      throw new InvalidNameException(
          "Name can not be empty!");
    }
    String subContextName = parsedName.get(0);
    Object boundObject = ctxMaps.get(parsedName.get(0));
    if (parsedName.size() == 1) {
      // Check if name is already in use
      if (boundObject == null) {
        Context subContext = new ContextImpl(this, subContextName);
        ctxMaps.put(subContextName, subContext);
        return subContext;
      } else {
        throw new NameAlreadyBoundException(
            String.format("Name %s is already bound!", subContextName));
      }
    } else {
      if (boundObject instanceof Context) {
        // Let the subcontext create new subcontext
        // lets consider a scenerio a/b/c
        // case a/b exists : c will be created
        // case a exists : b/c will not be created
        // an exception will be thrown in that case.
        return ((Context) boundObject).createSubcontext(parsedName.getSuffix(1));
      } else {
        throw new NotContextException(String.format("Expected Context but found %s",
            boundObject));
      }
    }
  }

  /**
   * Creates subcontext with name, relative to this Context.
   *
   * @param name subcontext name
   * @return new subcontext named name relative to this context.
   * @throws NamingException if naming error occurs.
   *
   */
  public Context createSubcontext(String name) throws NamingException {
    return createSubcontext(nameParser.parse(name));
  }

  /**
   * Destroys subcontext with name name. The subcontext must be empty otherwise
   * ContextNotEmptyException is thrown. Once a context is destroyed, the instance should not be
   * used.
   *
   * @param name subcontext to destroy
   * @throws NoPermissionException if this context has been destroyed.
   * @throws InvalidNameException if name is empty or is CompositeName that spans more than one
   *         naming system.
   * @throws ContextNotEmptyException if Context name is not empty.
   * @throws NameNotFoundException if subcontext with name name can not be found.
   * @throws NotContextException if name is not bound to instance of ContextImpl.
   *
   */
  public void destroySubcontext(Name name) throws NamingException {
    checkIsDestroyed();
    Name parsedName = getParsedName(name);
    if (parsedName.size() == 0 || parsedName.get(0).length() == 0) {
      throw new InvalidNameException(
          "Name can not be empty!");
    }
    String subContextName = parsedName.get(0);
    Object boundObject = ctxMaps.get(subContextName);
    if (boundObject == null) {
      throw new NameNotFoundException(String.format("Name %s not found in the context!",
          subContextName));
    }
    if (!(boundObject instanceof ContextImpl)) {
      throw new NotContextException();
    }
    ContextImpl contextToDestroy = (ContextImpl) boundObject;
    if (parsedName.size() == 1) {
      // Check if the Context to be destroyed is empty. Can not destroy
      // non-empty Context.
      if (contextToDestroy.ctxMaps.size() == 0) {
        ctxMaps.remove(subContextName);
        contextToDestroy.destroyInternal();
      } else {
        throw new ContextNotEmptyException(
            "Can not destroy non-empty Context!");
      }
    } else {
      // Let the subcontext destroy the context
      ((ContextImpl) boundObject).destroySubcontext(parsedName.getSuffix(1));
    }
  }

  /**
   * Destroys subcontext with name name.
   *
   * @param name name of subcontext to destroy.
   * @throws NamingException if naming error occurs
   */
  public void destroySubcontext(String name) throws NamingException {
    destroySubcontext(nameParser.parse(name));
  }

  /**
   * Not implemented
   *
   */
  public Hashtable getEnvironment() throws NamingException {
    throw new NamingException(
        "getEnvironment() is not implemented");
  }

  /**
   * Not implemented
   *
   */
  public String getNameInNamespace() throws NamingException {
    throw new NamingException(
        "getNameInNamespace() is not implemented");
  }

  /**
   * Retrieves NameParser used to parse context with name name.
   *
   * @param name context name.
   * @return NameParser.
   * @throws NoPermissionException if this context has been destroyed.
   * @throws NamingException if any other naming error occurs.
   *
   */
  public NameParser getNameParser(Name name) throws NamingException {
    checkIsDestroyed();
    return nameParser;
  }

  /**
   * Retrieves NameParser used to parse context with name name.
   *
   * @param name context name.
   * @return NameParser.
   * @throws NoPermissionException if this context has been destroyed.
   * @throws NamingException if any other naming error occurs.
   *
   */
  public NameParser getNameParser(String name) throws NamingException {
    checkIsDestroyed();
    return nameParser;
  }

  /**
   * The same as listBindings(String).
   *
   * @param name name of Context, relative to this Context
   * @return NamingEnumeration of all name-class pairs. Each element from the enumeration is
   *         instance of NameClassPair
   * @throws NamingException if naming error occurs
   *
   */
  public NamingEnumeration list(Name name) throws NamingException {
    return listBindings(name);
  }

  /**
   * The same as listBindings(String).
   *
   * @param name name of Context, relative to this Context
   * @return NamingEnumeration of all name-class pairs. Each element from the enumeration is
   *         instance of NameClassPair
   * @throws NamingException if naming error occurs
   *
   */
  public NamingEnumeration list(String name) throws NamingException {
    return list(nameParser.parse(name));
  }

  /**
   * Lists all bindings for Context with name name. If name is empty then this Context is assumed.
   *
   * @param name name of Context, relative to this Context
   * @return NamingEnumeration of all name-object pairs. Each element from the enumeration is
   *         instance of Binding.
   * @throws NoPermissionException if this context has been destroyed
   * @throws InvalidNameException if name is CompositeName that spans more than one naming system
   * @throws NameNotFoundException if name can not be found
   * @throws NotContextException component of name is not bound to instance of ContextImpl, when
   *         name is not an atomic name
   * @throws NamingException if any other naming error occurs
   *
   */
  public NamingEnumeration listBindings(Name name) throws NamingException {
    checkIsDestroyed();
    Name parsedName = getParsedName(name);
    if (parsedName.size() == 0) {
      Vector bindings = new Vector();
      Iterator iterat = ctxMaps.keySet().iterator();
      while (iterat.hasNext()) {
        String bindingName = (String) iterat.next();
        bindings.addElement(new Binding(bindingName, ctxMaps.get(bindingName)));
      }
      return new NamingEnumerationImpl(bindings);
    } else {
      Object subContext = ctxMaps.get(parsedName.get(0));
      if (subContext instanceof Context) {
        Name nextLayer = nameParser.parse("");
        // getSuffix(1) only apply to name with size() > 1
        if (parsedName.size() > 1) {
          nextLayer = parsedName.getSuffix(1);
        }
        return ((Context) subContext).list(nextLayer);
      }
      if (subContext == null && !ctxMaps.containsKey(parsedName.get(0))) {
        throw new NameNotFoundException(
            String.format("Name %s not found", name));
      } else {
        throw new NotContextException(String.format("Expected Context but found %s",
            subContext));
      }
    }
  }

  /**
   * Lists all bindings for Context with name name. If name is empty then this Context is assumed.
   *
   * @param name name of Context, relative to this Context
   * @return NamingEnumeration of all name-object pairs. Each element from the enumeration is
   *         instance of Binding.
   * @throws NoPermissionException if this context has been destroyed
   * @throws InvalidNameException if name is CompositeName that spans more than one naming system
   * @throws NameNotFoundException if name can not be found
   * @throws NotContextException component of name is not bound to instance of ContextImpl, when
   *         name is not an atomic name
   * @throws NamingException if any other naming error occurs
   *
   */
  public NamingEnumeration listBindings(String name) throws NamingException {
    return listBindings(nameParser.parse(name));
  }

  /**
   * Looks up object with binding name name in this context.
   *
   * @param name name to look up
   * @return object reference bound to name name.
   * @throws NoPermissionException if this context has been destroyed.
   * @throws InvalidNameException if name is CompositeName that spans more than one naming system
   * @throws NameNotFoundException if name can not be found.
   * @throws NotContextException component of name is not bound to instance of ContextImpl, when
   *         name is not atomic name.
   * @throws NamingException if any other naming error occurs
   *
   */
  public Object lookup(Name name) throws NamingException {
    checkIsDestroyed();
    try {
      Name parsedName = getParsedName(name);
      String nameComponent = parsedName.get(0);
      Object res = ctxMaps.get(nameComponent);
      if (res instanceof UserTransactionImpl) {
        res = new UserTransactionImpl();
      }
      // if not found
      if (!ctxMaps.containsKey(nameComponent)) {
        throw new NameNotFoundException(
            String.format("Name %s not found", name));
      }
      // if this is a compound name
      else if (parsedName.size() > 1) {
        if (res instanceof ContextImpl) {
          res = ((ContextImpl) res).lookup(parsedName.getSuffix(1));
        } else {
          throw new NotContextException(
              String.format("Expected ContextImpl but found %s", res));
        }
      }
      return res;
    } catch (NameNotFoundException e) {
      LogWriter writer = TransactionUtils.getLogWriter();
      if (writer.infoEnabled())
        writer.info(String.format("ContextImpl::lookup::Error while looking up %s", name),
            e);
      throw new NameNotFoundException(
          String.format("Name %s not found", name));
    } catch (SystemException se) {
      LogWriter writer = TransactionUtils.getLogWriter();
      if (writer.severeEnabled())
        writer.info(
            "ContextImpl::lookup::Error while creating UserTransaction object",
            se);
      throw new NameNotFoundException(
          "ContextImpl::lookup::Error while creating UserTransaction object");
    }
  }

  /**
   * Looks up the object in this context. If the object is not found and the remote context was
   * provided, calls the remote context to lookup the object.
   *
   * @param name object to search
   * @return object reference bound to name name.
   * @throws NamingException if naming error occurs.
   *
   */
  public Object lookup(String name) throws NamingException {
    checkIsDestroyed();
    try {
      return lookup(nameParser.parse(name));
    } catch (NameNotFoundException e) {
      LogWriter writer = TransactionUtils.getLogWriter();
      if (writer.infoEnabled())
        writer.info(String.format("ContextImpl::lookup::Error while looking up %s", name),
            e);
      throw new NameNotFoundException(
          String.format("Name %s not found", new Object[] {name}));
    }
  }

  /**
   * Not implemented
   *
   */
  public Object lookupLink(Name name) throws NamingException {
    throw new NamingException(
        "lookupLink(Name name) is not implemented");
  }

  /**
   * Not implemented
   *
   */
  public Object lookupLink(String name) throws NamingException {
    throw new NamingException(
        "lookupLink(String name) is not implemented");
  }

  /**
   * Rebinds object obj to name name . If there is existing binding it will be overwritten.
   *
   * @param name name of the object to rebind.
   * @param obj object to add. Can be null.
   * @throws NoPermissionException if this context has been destroyed
   * @throws InvalidNameException if name is empty or is CompositeName that spans more than one
   *         naming system
   * @throws NotContextException if name has more than one atomic name and intermediate context is
   *         not found
   * @throws NamingException if any other naming error occurs
   *
   */
  public void rebind(Name name, Object obj) throws NamingException {
    checkIsDestroyed();
    Name parsedName = getParsedName(name);
    if (parsedName.size() == 0 || parsedName.get(0).length() == 0) {
      throw new InvalidNameException(
          "Name can not be empty!");
    }
    String nameToBind = parsedName.get(0);
    if (parsedName.size() == 1) {
      ctxMaps.put(nameToBind, obj);
    } else {
      Object boundObject = ctxMaps.get(nameToBind);
      if (boundObject instanceof Context) {
        /*
         * Let the subcontext bind the object.
         */
        ((Context) boundObject).bind(parsedName.getSuffix(1), obj);
      } else {
        if (boundObject == null) {
          // Create new subcontext and let it do the binding
          Context sub = createSubcontext(nameToBind);
          sub.bind(parsedName.getSuffix(1), obj);
        } else {
          throw new NotContextException(String.format("Expected Context but found %s",
              boundObject));
        }
      }
    }
  }

  /**
   * Rebinds object obj to String name. If there is existing binding it will be overwritten.
   *
   * @param name name of the object to rebind.
   * @param obj object to add. Can be null.
   * @throws NoPermissionException if this context has been destroyed
   * @throws InvalidNameException if name is empty or is CompositeName that spans more than one
   *         naming system
   * @throws NotContextException if name has more than one atomic name and intermediate context is
   *         not found
   * @throws NamingException if any other naming error occurs
   *
   */
  public void rebind(String name, Object obj) throws NamingException {
    rebind(nameParser.parse(name), obj);
  }

  /**
   * Not implemented
   *
   */
  public Object removeFromEnvironment(String key) throws NamingException {
    throw new NamingException(
        "removeFromEnvironment(String key) is not implemented");
  }

  /**
   * Not implemented
   *
   */
  public void rename(Name name1, Name name2) throws NamingException {
    throw new NamingException(
        "rename(Name name1, Name name2) is not implemented");
  }

  /**
   * Not implemented
   *
   */
  public void rename(String name1, String name2) throws NamingException {
    throw new NamingException(
        "rename(String name1, String name2) is not implemented");
  }

  /**
   * Removes name and its associated object from the context.
   *
   * @param name name to remove
   * @throws NoPermissionException if this context has been destroyed.
   * @throws InvalidNameException if name is empty or is CompositeName that spans more than one
   *         naming system
   * @throws NameNotFoundException if intermediate context can not be found
   * @throws NotContextException if name has more than one atomic name and intermediate context is
   *         not found.
   * @throws NamingException if any other naming exception occurs
   *
   */
  public void unbind(Name name) throws NamingException {
    checkIsDestroyed();
    Name parsedName = getParsedName(name);
    if (parsedName.size() == 0 || parsedName.get(0).length() == 0) {
      throw new InvalidNameException(
          "Name can not be empty!");
    }
    String nameToRemove = parsedName.get(0);
    // scenerio unbind a
    // remove a and its associated object
    if (parsedName.size() == 1) {
      ctxMaps.remove(nameToRemove);
    } else {
      // scenerio unbind a/b or a/b/c
      // remove b and its associated object
      Object boundObject = ctxMaps.get(nameToRemove);
      if (boundObject instanceof Context) {
        // remove b and its associated object
        ((Context) boundObject).unbind(parsedName.getSuffix(1));
      } else {
        // if the name is not found then throw exception
        if (!ctxMaps.containsKey(nameToRemove)) {
          throw new NameNotFoundException(
              String.format("Can not find %s", name));
        }
        throw new NotContextException(String.format("Expected Context but found %s",
            boundObject));
      }
    }
  }

  /**
   *
   * Removes name and its associated object from the context.
   *
   * @param name name to remove
   * @throws NoPermissionException if this context has been destroyed.
   * @throws InvalidNameException if name is empty or is CompositeName that spans more than one
   *         naming system
   * @throws NameNotFoundException if intermediate context can not be found
   * @throws NotContextException if name has more than one atomic name and intermediate context is
   *         not found.
   * @throws NamingException if any other naming exception occurs
   *
   */
  public void unbind(String name) throws NamingException {
    unbind(nameParser.parse(name));
  }

  /**
   * Checks if this context has been destroyed. isDestroyed is set to true when a context is
   * destroyed by calling destroySubcontext method.
   *
   * @throws NoPermissionException if this context has been destroyed
   */
  private void checkIsDestroyed() throws NamingException {
    if (isDestroyed) {
      throw new NoPermissionException(
          "Can not invoke operations on destroyed context!");
    }
  }

  /**
   * Marks this context as destroyed. Method called only by destroySubcontext.
   */
  private void destroyInternal() {
    isDestroyed = true;
  }

  /**
   * Parses name which is CompositeName or CompoundName . If name is not CompositeName then it is
   * assumed to be CompoundName. If the name contains leading and/or terminal empty components, they
   * will not be included in the result.
   *
   * @param name Name to parse.
   * @return parsed name as instance of CompoundName.
   * @throws InvalidNameException if name is CompositeName and spans more than one name space.
   * @throws NamingException if any other naming exception occurs
   */
  private Name getParsedName(Name name) throws NamingException {
    Name result = null;
    if (name instanceof CompositeName) {
      if (name.size() == 0) {
        // Return empty CompositeName
        result = nameParser.parse("");
      } else if (name.size() > 1) {
        throw new InvalidNameException(
            "Multiple name systems are not supported!");
      }
      result = nameParser.parse(name.get(0));
    } else {
      result = (Name) name.clone();
    }
    while (!result.isEmpty()) {
      if (result.get(0).length() == 0) {
        result.remove(0);
        continue;
      }
      if (result.get(result.size() - 1).length() == 0) {
        result.remove(result.size() - 1);
        continue;
      }
      break;
    }
    // Validate that there are not intermediate empty components.
    // Skip first and last element, they are valid
    for (int i = 1; i < result.size() - 1; i++) {
      if (result.get(i).length() == 0) {
        throw new InvalidNameException(
            "Empty intermediate components are not supported!");
      }
    }
    return result;
  }

  /**
   * Returns the compound string name of this context. Suppose a/b/c/d is the full name and this
   * context is "c". It's compound string name is a/b/c
   *
   * @return compound string name of the context
   */
  String getCompoundStringName() throws NamingException {
    // StringBuffer compositeName = new StringBuffer();
    String compositeName = "";
    ContextImpl curCtx = this;
    while (!curCtx.isRootContext()) {
      compositeName = composeName(compositeName, curCtx.getAtomicName());
      curCtx = curCtx.getParentContext();
    }
    return compositeName;
  }

  /*
   * Returns parent context of this context
   */
  ContextImpl getParentContext() {
    return parentCtx;
  }

  /*
   * Returns the "atomic" (as opposed to "composite") name of the context.
   *
   * @return name of the context
   */
  String getAtomicName() {
    return ctxName;
  }

  /*
   * Returns true if this context is the root context @return true if the context is the root
   * context
   */
  boolean isRootContext() {
    return getParentContext() == null;
  }

  private static class NamingEnumerationImpl implements NamingEnumeration {

    private Vector elements;
    private int currentElement;

    NamingEnumerationImpl(Vector elements) {
      this.elements = elements;
      this.currentElement = 0;
    }

    public void close() {
      currentElement = 0;
      elements.clear();
    }

    public boolean hasMore() {
      return hasMoreElements();
    }

    public boolean hasMoreElements() {
      if (currentElement < elements.size()) {
        return true;
      }
      close();
      return false;
    }

    public Object next() {
      return nextElement();
    }

    public Object nextElement() {
      if (hasMoreElements()) {
        return elements.get(currentElement++);
      }
      throw new NoSuchElementException();
    }
  }
}
