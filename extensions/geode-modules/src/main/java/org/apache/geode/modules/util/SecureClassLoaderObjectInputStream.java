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
package org.apache.geode.modules.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputFilter;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A secure version of ClassLoaderObjectInputStream that includes deserialization filtering
 * to prevent unsafe deserialization attacks.
 *
 * <p>
 * This class extends the original ClassLoaderObjectInputStream with security features:
 * <ul>
 * <li>Mandatory ObjectInputFilter for class validation</li>
 * <li>Security logging for deserialization attempts</li>
 * <li>Defense against known gadget chain attacks</li>
 * </ul>
 *
 * <p>
 * <b>Usage Example:</b>
 *
 * <pre>
 * ObjectInputFilter filter = new SafeDeserializationFilter();
 * ObjectInputStream ois = new SecureClassLoaderObjectInputStream(
 *     inputStream, classLoader, filter);
 * Object obj = ois.readObject();
 * </pre>
 *
 * @since 1.15.0
 * @see ClassLoaderObjectInputStream
 * @see org.apache.geode.modules.session.filter.SafeDeserializationFilter
 */
public class SecureClassLoaderObjectInputStream extends ObjectInputStream {

  private static final Logger LOG =
      LoggerFactory.getLogger(SecureClassLoaderObjectInputStream.class);
  private static final Logger SECURITY_LOG =
      LoggerFactory.getLogger("org.apache.geode.security.deserialization");

  private final ClassLoader loader;
  private final ObjectInputFilter filter;

  /**
   * Creates a SecureClassLoaderObjectInputStream with required security filter.
   *
   * <p>
   * <b>Security Design - Fail-Safe Approach:</b><br>
   * This constructor requires a non-null ObjectInputFilter as a mandatory security control.
   * If no filter is provided, the constructor fails immediately rather than allowing unsafe
   * deserialization. This "fail-safe" design prevents accidental use without security filtering.
   *
   * <p>
   * <b>Why Filtering is Mandatory:</b><br>
   * Without an ObjectInputFilter, this class would be vulnerable to:
   * <ul>
   * <li>Remote Code Execution (RCE) via gadget chain attacks</li>
   * <li>Denial of Service (DoS) via resource exhaustion</li>
   * <li>Information disclosure via exception-based attacks</li>
   * </ul>
   *
   * <p>
   * The filter is installed immediately via {@code setObjectInputFilter()} before any
   * deserialization can occur, ensuring no window of vulnerability exists.
   *
   * @param in the input stream to read from (typically from session storage)
   * @param loader the ClassLoader to use for class resolution (web app classloader)
   * @param filter the ObjectInputFilter for security validation (must not be null)
   * @throws IOException if an I/O error occurs while reading the stream header
   * @throws IllegalArgumentException if filter is null (fail-safe security check)
   */
  public SecureClassLoaderObjectInputStream(InputStream in, ClassLoader loader,
      ObjectInputFilter filter) throws IOException {
    super(in);

    // SECURITY: Fail-safe check - never allow deserialization without filtering
    // This prevents developer error or malicious removal of the filter
    if (filter == null) {
      throw new IllegalArgumentException(
          "ObjectInputFilter must not be null - deserialization without filtering is unsafe");
    }

    this.loader = loader;
    this.filter = filter;

    // Set the filter on this stream - must be done before any readObject() calls
    // This ensures the filter is active for all deserialization operations
    setObjectInputFilter(filter);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Created SecureClassLoaderObjectInputStream with filter: {}",
          filter.getClass().getName());
    }
  }

  /**
   * Resolves a class descriptor to a Class object during deserialization.
   *
   * <p>
   * <b>Class Resolution Strategy:</b><br>
   * This method uses a two-tier class loading approach:
   * <ol>
   * <li><b>Primary:</b> Use the provided ClassLoader (typically the web application's
   * classloader) to resolve application-specific classes</li>
   * <li><b>Fallback:</b> Use the thread context ClassLoader if the primary fails</li>
   * </ol>
   *
   * <p>
   * <b>Security Considerations:</b><br>
   * Class resolution happens AFTER the ObjectInputFilter has validated the class name.
   * The filter (SafeDeserializationFilter) blocks dangerous classes before this method
   * is called, so this method only handles legitimate classes that passed filtering.
   *
   * <p>
   * <b>Security Logging:</b><br>
   * Failed class resolutions are logged to the security logger because they may indicate:
   * <ul>
   * <li>Attempted deserialization of non-existent classes (exploit probing)</li>
   * <li>Class loader manipulation attacks</li>
   * <li>Deployment configuration issues (legitimate failures)</li>
   * </ul>
   *
   * @param desc the class descriptor from the serialization stream
   * @return the resolved Class object
   * @throws ClassNotFoundException if the class cannot be found in any classloader
   */
  @Override
  public Class<?> resolveClass(ObjectStreamClass desc) throws ClassNotFoundException {
    String className = desc.getName();

    if (LOG.isTraceEnabled()) {
      LOG.trace("Resolving class for deserialization: {}", className);
    }

    Class<?> theClass;
    try {
      // Try to load with the provided ClassLoader (web app classloader)
      // This allows deserialization of application-specific classes
      theClass = Class.forName(className, false, loader);

      if (LOG.isDebugEnabled()) {
        LOG.debug("Successfully resolved class {} using provided ClassLoader", className);
      }
    } catch (ClassNotFoundException cnfe) {
      // Fallback to thread context ClassLoader
      // This is needed for classes not visible to the provided classloader
      LOG.debug("Class {} not found with provided ClassLoader, trying thread context ClassLoader",
          className);

      ClassLoader contextLoader = Thread.currentThread().getContextClassLoader();
      if (contextLoader != null) {
        try {
          theClass = contextLoader.loadClass(className);
          LOG.debug("Successfully resolved class {} using context ClassLoader", className);
        } catch (ClassNotFoundException cnfe2) {
          // SECURITY: Log failed class resolutions - may indicate exploit attempts
          SECURITY_LOG.warn("Failed to resolve class: {} - may indicate attempted exploit",
              className);
          throw cnfe2;
        }
      } else {
        SECURITY_LOG.warn("Failed to resolve class: {} - no context ClassLoader available",
            className);
        throw cnfe;
      }
    }

    return theClass;
  }

  /**
   * Resolves a dynamic proxy class during deserialization.
   *
   * <p>
   * <b>Purpose:</b><br>
   * This method handles deserialization of Java dynamic proxy objects (created via
   * java.lang.reflect.Proxy). Dynamic proxies are commonly used for lazy loading,
   * RPC frameworks, and AOP (aspect-oriented programming).
   *
   * <p>
   * <b>Security Note:</b><br>
   * Dynamic proxies themselves are not inherently dangerous, but they can be part of
   * exploit chains. The ObjectInputFilter (SafeDeserializationFilter) validates the
   * proxy's interfaces before this method is called, blocking dangerous combinations.
   *
   * <p>
   * <b>ClassLoader Handling:</b><br>
   * This method follows Java's standard proxy resolution rules:
   * <ul>
   * <li>If all interfaces are public: use the provided ClassLoader</li>
   * <li>If any interface is non-public: use that interface's ClassLoader</li>
   * <li>If multiple non-public interfaces: they must be from the same ClassLoader</li>
   * </ul>
   *
   * @param interfaces array of interface names that the proxy implements
   * @return the resolved proxy Class
   * @throws IOException if an I/O error occurs
   * @throws ClassNotFoundException if any interface cannot be found
   * @throws IllegalAccessError if multiple non-public interfaces from different classloaders
   */
  @Override
  protected Class<?> resolveProxyClass(String[] interfaces)
      throws IOException, ClassNotFoundException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Resolving proxy class with interfaces: {}", String.join(", ", interfaces));
    }

    // Load interface classes using the provided ClassLoader
    ClassLoader nonPublicLoader = null;
    boolean hasNonPublicInterface = false;

    // First pass: load all interface classes and check visibility
    Class<?>[] classObjs = new Class<?>[interfaces.length];
    for (int i = 0; i < interfaces.length; i++) {
      Class<?> cl = Class.forName(interfaces[i], false, loader);

      // Check if this interface is non-public (package-private)
      if ((cl.getModifiers() & java.lang.reflect.Modifier.PUBLIC) == 0) {
        if (hasNonPublicInterface) {
          // Multiple non-public interfaces must be from the same classloader
          if (nonPublicLoader != cl.getClassLoader()) {
            throw new IllegalAccessError(
                "conflicting non-public interface class loaders");
          }
        } else {
          nonPublicLoader = cl.getClassLoader();
          hasNonPublicInterface = true;
        }
      }
      classObjs[i] = cl;
    }

    try {
      // Use the non-public interface's classloader if needed, otherwise use provided loader
      ClassLoader loaderToUse = hasNonPublicInterface ? nonPublicLoader : loader;
      @SuppressWarnings("deprecation")
      Class<?> proxyClass = java.lang.reflect.Proxy.getProxyClass(loaderToUse, classObjs);
      return proxyClass;
    } catch (IllegalArgumentException e) {
      throw new ClassNotFoundException("Proxy class creation failed", e);
    }
  }

  /**
   * Gets the configured ObjectInputFilter
   *
   * @return the filter being used
   */
  public ObjectInputFilter getFilter() {
    return filter;
  }

  /**
   * Gets the ClassLoader being used
   *
   * @return the ClassLoader
   */
  public ClassLoader getClassLoader() {
    return loader;
  }
}
