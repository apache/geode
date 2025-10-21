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
package org.apache.geode.modules.session.filter;

import java.io.ObjectInputFilter;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Security filter for safe deserialization of session attributes.
 *
 * <p>
 * This filter prevents unsafe deserialization attacks by implementing a strict whitelist
 * of allowed classes and blocking known dangerous classes that can be used in gadget chain
 * attacks.
 *
 * <p>
 * <b>Security Features:</b>
 * <ul>
 * <li>Whitelist-based class filtering (default-deny)</li>
 * <li>Blocks known dangerous classes (e.g., Commons Collections, Spring internals)</li>
 * <li>Configurable depth and size limits</li>
 * <li>Comprehensive security logging</li>
 * </ul>
 *
 * @since 1.15.0
 */
public class SafeDeserializationFilter implements ObjectInputFilter {

  private static final Logger LOG = LoggerFactory.getLogger(SafeDeserializationFilter.class);
  private static final Logger SECURITY_LOG =
      LoggerFactory.getLogger("org.apache.geode.security.deserialization");

  // Maximum object graph depth to prevent stack overflow attacks
  private static final long MAX_DEPTH = 50;

  // Maximum number of object references to prevent memory exhaustion
  private static final long MAX_REFERENCES = 10000;

  // Maximum array size to prevent memory exhaustion
  private static final long MAX_ARRAY_SIZE = 10000;

  // Maximum total bytes to prevent resource exhaustion
  private static final long MAX_BYTES = 10_000_000; // 10MB

  /**
   * Known dangerous classes that are commonly used in deserialization gadget chains.
   * These should NEVER be deserialized from untrusted sources.
   */
  private static final Set<String> BLOCKED_CLASSES = new HashSet<>();

  /**
   * Patterns for dangerous class prefixes
   */
  private static final Set<Pattern> BLOCKED_PATTERNS = new HashSet<>();

  /**
   * Allowed class patterns (whitelist)
   */
  private static final Set<Pattern> ALLOWED_PATTERNS = new HashSet<>();

  static {
    // Block known gadget chain classes

    // Apache Commons Collections - TransformedMap, LazyMap exploits
    BLOCKED_CLASSES.add("org.apache.commons.collections.functors.InvokerTransformer");
    BLOCKED_CLASSES.add("org.apache.commons.collections.functors.ChainedTransformer");
    BLOCKED_CLASSES.add("org.apache.commons.collections.functors.ConstantTransformer");
    BLOCKED_CLASSES.add("org.apache.commons.collections.functors.InstantiateTransformer");
    BLOCKED_CLASSES.add("org.apache.commons.collections.keyvalue.TiedMapEntry");
    BLOCKED_CLASSES.add("org.apache.commons.collections.map.LazyMap");
    BLOCKED_CLASSES.add("org.apache.commons.collections4.functors.InvokerTransformer");
    BLOCKED_CLASSES.add("org.apache.commons.collections4.functors.ChainedTransformer");
    BLOCKED_CLASSES.add("org.apache.commons.collections4.functors.ConstantTransformer");
    BLOCKED_CLASSES.add("org.apache.commons.collections4.functors.InstantiateTransformer");
    BLOCKED_CLASSES.add("org.apache.commons.collections4.keyvalue.TiedMapEntry");
    BLOCKED_CLASSES.add("org.apache.commons.collections4.map.LazyMap");

    // Spring Framework - BeanFactory exploits
    BLOCKED_CLASSES.add("org.springframework.beans.factory.ObjectFactory");
    BLOCKED_CLASSES
        .add("org.springframework.core.SerializableTypeWrapper$MethodInvokeTypeProvider");
    BLOCKED_CLASSES.add("org.springframework.aop.framework.AdvisedSupport");
    BLOCKED_CLASSES.add("org.springframework.aop.target.SingletonTargetSource");

    // JDK internal classes that can be exploited
    BLOCKED_CLASSES.add("com.sun.org.apache.xalan.internal.xsltc.trax.TemplatesImpl");
    BLOCKED_CLASSES.add("javax.management.BadAttributeValueExpException");
    BLOCKED_CLASSES.add("java.rmi.server.UnicastRemoteObject");
    BLOCKED_CLASSES.add("java.rmi.server.RemoteObjectInvocationHandler");

    // Groovy exploits
    BLOCKED_CLASSES.add("org.codehaus.groovy.runtime.ConvertedClosure");
    BLOCKED_CLASSES.add("org.codehaus.groovy.runtime.MethodClosure");

    // C3P0 JNDI exploits
    BLOCKED_CLASSES.add("com.mchange.v2.c3p0.impl.PoolBackedDataSourceBase");
    BLOCKED_CLASSES.add("com.mchange.v2.c3p0.JndiRefForwardingDataSource");

    // Hibernate exploits
    BLOCKED_CLASSES.add("org.hibernate.jmx.StatisticsService");
    BLOCKED_CLASSES.add("org.hibernate.engine.spi.TypedValue");

    // Block dangerous package patterns
    BLOCKED_PATTERNS.add(Pattern.compile("^org\\.apache\\.commons\\.collections\\.functors\\..*"));
    BLOCKED_PATTERNS.add(Pattern.compile("^org\\.apache\\.commons\\.collections4\\.functors\\..*"));
    BLOCKED_PATTERNS.add(Pattern.compile("^org\\.springframework\\.beans\\.factory\\..*"));
    BLOCKED_PATTERNS.add(Pattern.compile("^org\\.springframework\\.aop\\..*"));
    BLOCKED_PATTERNS.add(Pattern.compile("^com\\.sun\\.org\\.apache\\.xalan\\..*"));
    BLOCKED_PATTERNS.add(Pattern.compile("^javax\\.management\\..*"));
    BLOCKED_PATTERNS.add(Pattern.compile("^java\\.rmi\\..*"));
    BLOCKED_PATTERNS.add(Pattern.compile("^sun\\.rmi\\..*"));
    BLOCKED_PATTERNS.add(Pattern.compile("^org\\.codehaus\\.groovy\\.runtime\\..*"));
    BLOCKED_PATTERNS.add(Pattern.compile("^com\\.mchange\\.v2\\.c3p0\\..*"));

    // Allow safe Java classes
    ALLOWED_PATTERNS.add(Pattern.compile("^java\\.lang\\.String$"));
    ALLOWED_PATTERNS.add(Pattern.compile("^java\\.lang\\.Number$"));
    ALLOWED_PATTERNS.add(Pattern.compile("^java\\.lang\\.Integer$"));
    ALLOWED_PATTERNS.add(Pattern.compile("^java\\.lang\\.Long$"));
    ALLOWED_PATTERNS.add(Pattern.compile("^java\\.lang\\.Float$"));
    ALLOWED_PATTERNS.add(Pattern.compile("^java\\.lang\\.Double$"));
    ALLOWED_PATTERNS.add(Pattern.compile("^java\\.lang\\.Boolean$"));
    ALLOWED_PATTERNS.add(Pattern.compile("^java\\.lang\\.Byte$"));
    ALLOWED_PATTERNS.add(Pattern.compile("^java\\.lang\\.Short$"));
    ALLOWED_PATTERNS.add(Pattern.compile("^java\\.lang\\.Character$"));
    ALLOWED_PATTERNS.add(Pattern.compile("^java\\.math\\.BigInteger$"));
    ALLOWED_PATTERNS.add(Pattern.compile("^java\\.math\\.BigDecimal$"));

    // Allow safe collection classes
    ALLOWED_PATTERNS.add(Pattern.compile("^java\\.util\\.ArrayList$"));
    ALLOWED_PATTERNS.add(Pattern.compile("^java\\.util\\.LinkedList$"));
    ALLOWED_PATTERNS.add(Pattern.compile("^java\\.util\\.HashMap$"));
    ALLOWED_PATTERNS.add(Pattern.compile("^java\\.util\\.LinkedHashMap$"));
    ALLOWED_PATTERNS.add(Pattern.compile("^java\\.util\\.TreeMap$"));
    ALLOWED_PATTERNS.add(Pattern.compile("^java\\.util\\.HashSet$"));
    ALLOWED_PATTERNS.add(Pattern.compile("^java\\.util\\.LinkedHashSet$"));
    ALLOWED_PATTERNS.add(Pattern.compile("^java\\.util\\.TreeSet$"));
    ALLOWED_PATTERNS.add(Pattern.compile("^java\\.util\\.concurrent\\.ConcurrentHashMap$"));
    ALLOWED_PATTERNS.add(Pattern.compile("^java\\.util\\.Collections\\$.*"));

    // Allow date/time classes
    ALLOWED_PATTERNS.add(Pattern.compile("^java\\.util\\.Date$"));
    ALLOWED_PATTERNS.add(Pattern.compile("^java\\.sql\\.Date$"));
    ALLOWED_PATTERNS.add(Pattern.compile("^java\\.sql\\.Time$"));
    ALLOWED_PATTERNS.add(Pattern.compile("^java\\.sql\\.Timestamp$"));
    ALLOWED_PATTERNS.add(Pattern.compile("^java\\.time\\..*"));

    // Allow arrays of primitives and safe classes
    ALLOWED_PATTERNS.add(Pattern.compile("^\\[Z$")); // boolean[]
    ALLOWED_PATTERNS.add(Pattern.compile("^\\[B$")); // byte[]
    ALLOWED_PATTERNS.add(Pattern.compile("^\\[C$")); // char[]
    ALLOWED_PATTERNS.add(Pattern.compile("^\\[S$")); // short[]
    ALLOWED_PATTERNS.add(Pattern.compile("^\\[I$")); // int[]
    ALLOWED_PATTERNS.add(Pattern.compile("^\\[J$")); // long[]
    ALLOWED_PATTERNS.add(Pattern.compile("^\\[F$")); // float[]
    ALLOWED_PATTERNS.add(Pattern.compile("^\\[D$")); // double[]
    ALLOWED_PATTERNS.add(Pattern.compile("^\\[Ljava\\.lang\\.String;$")); // String[]
    ALLOWED_PATTERNS.add(Pattern.compile("^\\[Ljava\\.lang\\.Object;$")); // Object[]
  }

  private final Set<Pattern> customAllowedPatterns;
  private final Set<String> customAllowedClasses;

  /**
   * Creates a filter with default configuration
   */
  public SafeDeserializationFilter() {
    this(new HashSet<>(), new HashSet<>());
  }

  /**
   * Creates a filter with custom allowed classes
   *
   * @param customAllowedClasses exact class names to allow
   * @param customAllowedPatterns regex patterns for allowed classes
   */
  public SafeDeserializationFilter(Set<String> customAllowedClasses,
      Set<Pattern> customAllowedPatterns) {
    this.customAllowedClasses = new HashSet<>(customAllowedClasses);
    this.customAllowedPatterns = new HashSet<>(customAllowedPatterns);

    LOG.info("Initialized SafeDeserializationFilter with {} custom classes and {} custom patterns",
        customAllowedClasses.size(), customAllowedPatterns.size());
  }

  /**
   * Main filtering method called during deserialization to validate each object.
   *
   * <p>
   * This method is invoked by the Java serialization framework for every object during
   * deserialization. It implements a defense-in-depth strategy:
   *
   * <ol>
   * <li><b>Resource Limits:</b> Prevents DoS attacks by limiting depth, references, array
   * sizes, and total bytes</li>
   * <li><b>Blacklist Check:</b> Rejects known gadget chain classes (e.g., InvokerTransformer,
   * TemplatesImpl)</li>
   * <li><b>Whitelist Check:</b> Only allows explicitly permitted Java classes (default-deny
   * policy)</li>
   * <li><b>Security Logging:</b> Records all rejected classes for audit trail</li>
   * </ol>
   *
   * <p>
   * <b>Attack Vectors Blocked:</b>
   * <ul>
   * <li>Apache Commons Collections gadget chains (TransformedMap, LazyMap exploits)</li>
   * <li>Spring Framework BeanFactory exploits</li>
   * <li>JDK internal class exploits (TemplatesImpl, RMI)</li>
   * <li>Groovy MethodClosure exploits</li>
   * <li>C3P0 JNDI injection attacks</li>
   * <li>Hibernate JMX exploits</li>
   * <li>Stack overflow attacks (depth limit)</li>
   * <li>Memory exhaustion attacks (size limits)</li>
   * </ul>
   *
   * @param filterInfo metadata about the object being deserialized (class, depth, size, etc.)
   * @return Status.ALLOWED if safe to deserialize, Status.REJECTED if dangerous
   */
  @Override
  public Status checkInput(FilterInfo filterInfo) {
    if (filterInfo == null) {
      return Status.REJECTED;
    }

    // Check depth limits - prevents stack overflow attacks by limiting object graph depth
    if (filterInfo.depth() > MAX_DEPTH) {
      logSecurityViolation("DEPTH_EXCEEDED",
          "Object graph depth " + filterInfo.depth() + " exceeds maximum " + MAX_DEPTH,
          filterInfo);
      return Status.REJECTED;
    }

    // Check reference limits - prevents memory exhaustion from circular references
    // Limits total number of object references to prevent heap exhaustion attacks
    if (filterInfo.references() > MAX_REFERENCES) {
      logSecurityViolation("REFERENCES_EXCEEDED",
          "Object reference count " + filterInfo.references() + " exceeds maximum "
              + MAX_REFERENCES,
          filterInfo);
      return Status.REJECTED;
    }

    // Check array size limits - prevents massive array allocations
    // Large arrays can cause OutOfMemoryError and denial of service
    if (filterInfo.arrayLength() > MAX_ARRAY_SIZE) {
      logSecurityViolation("ARRAY_SIZE_EXCEEDED",
          "Array size " + filterInfo.arrayLength() + " exceeds maximum " + MAX_ARRAY_SIZE,
          filterInfo);
      return Status.REJECTED;
    }

    // Check total bytes - prevents resource exhaustion
    // Limits total deserialization payload size to 10MB
    if (filterInfo.streamBytes() > MAX_BYTES) {
      logSecurityViolation("BYTES_EXCEEDED",
          "Stream bytes " + filterInfo.streamBytes() + " exceeds maximum " + MAX_BYTES,
          filterInfo);
      return Status.REJECTED;
    }

    // Check class-specific rules - validate class against blocklist and whitelist
    Class<?> serialClass = filterInfo.serialClass();
    if (serialClass != null) {
      String className = serialClass.getName();

      // First, check if it's an explicitly blocked class - reject known gadget chain classes
      // These classes are NEVER safe to deserialize from untrusted sources
      if (BLOCKED_CLASSES.contains(className)) {
        logSecurityViolation("BLOCKED_CLASS",
            "Class " + className + " is explicitly blocked (known gadget chain)",
            filterInfo);
        return Status.REJECTED;
      }

      // Check against blocked patterns - reject classes matching dangerous package patterns
      // This catches variants and new versions of known exploit classes
      for (Pattern pattern : BLOCKED_PATTERNS) {
        if (pattern.matcher(className).matches()) {
          logSecurityViolation("BLOCKED_PATTERN",
              "Class " + className + " matches blocked pattern: " + pattern.pattern(),
              filterInfo);
          return Status.REJECTED;
        }
      }

      // Check if class is in allowed list - whitelist approach (default-deny policy)
      // Only classes explicitly permitted can be deserialized
      if (isClassAllowed(className)) {
        LOG.debug("Allowing deserialization of class: {}", className);
        return Status.ALLOWED;
      }

      // If not explicitly allowed, reject (whitelist approach)
      // This is the safest approach - all classes are untrusted by default
      logSecurityViolation("NOT_WHITELISTED",
          "Class " + className + " is not in the whitelist",
          filterInfo);
      return Status.REJECTED;
    }

    // Allow primitives and basic types (int, long, etc.)
    // UNDECIDED means let the framework continue with default behavior
    return Status.UNDECIDED;
  }

  /**
   * Checks if a class name is allowed for deserialization.
   *
   * <p>
   * This method implements the whitelist check by matching the class name against:
   * <ol>
   * <li>Custom allowed classes (exact string match)</li>
   * <li>Custom allowed patterns (regex match)</li>
   * <li>Default allowed patterns (safe Java standard library classes)</li>
   * </ol>
   *
   * @param className fully qualified class name to check
   * @return true if the class is whitelisted, false otherwise
   */
  private boolean isClassAllowed(String className) {
    // Check exact matches in custom allowed classes
    if (customAllowedClasses.contains(className)) {
      return true;
    }

    // Check against default allowed patterns
    for (Pattern pattern : ALLOWED_PATTERNS) {
      if (pattern.matcher(className).matches()) {
        return true;
      }
    }

    // Check against custom allowed patterns
    for (Pattern pattern : customAllowedPatterns) {
      if (pattern.matcher(className).matches()) {
        return true;
      }
    }

    return false;
  }

  /**
   * Logs security violations with detailed information for audit trail.
   *
   * <p>
   * Security violations are logged to both:
   * <ul>
   * <li><b>org.apache.geode.security.deserialization</b> logger (ERROR level) - dedicated
   * security log for compliance/audit</li>
   * <li><b>SafeDeserializationFilter</b> logger (WARN level) - operational monitoring</li>
   * </ul>
   *
   * <p>
   * Log entries include:
   * <ul>
   * <li>Violation type (BLOCKED_CLASS, DEPTH_EXCEEDED, etc.)</li>
   * <li>Descriptive message</li>
   * <li>Class name being deserialized</li>
   * <li>Object graph metrics (depth, references, array size, bytes)</li>
   * </ul>
   *
   * <p>
   * These logs enable:
   * <ul>
   * <li>Security incident detection and response</li>
   * <li>Compliance auditing (SOC2, PCI-DSS)</li>
   * <li>Attack pattern analysis</li>
   * <li>False positive investigation</li>
   * </ul>
   *
   * @param violationType category of security violation (e.g., "BLOCKED_CLASS")
   * @param message human-readable description of the violation
   * @param filterInfo deserialization context with object metrics
   */
  private void logSecurityViolation(String violationType, String message, FilterInfo filterInfo) {
    SECURITY_LOG.error("SECURITY ALERT - Deserialization Attempt Blocked: {} - {} - " +
        "Class: {}, Depth: {}, References: {}, ArrayLength: {}, StreamBytes: {}",
        violationType,
        message,
        filterInfo.serialClass() != null ? filterInfo.serialClass().getName() : "null",
        filterInfo.depth(),
        filterInfo.references(),
        filterInfo.arrayLength(),
        filterInfo.streamBytes());

    // Also log to standard logger for visibility
    LOG.warn("Blocked deserialization attempt: {} - {}", violationType, message);
  }

  /**
   * Factory method to create a filter with additional allowed classes.
   *
   * <p>
   * Use this when you need to deserialize application-specific classes beyond the
   * default whitelist. Only add classes you fully trust and control.
   *
   * <p>
   * <b>Security Warning:</b> Adding classes to the whitelist increases attack surface.
   * Only add classes that:
   * <ul>
   * <li>Are part of your application (not third-party libraries)</li>
   * <li>Have been reviewed for safe deserialization</li>
   * <li>Do not contain dangerous methods (execute, invoke, eval, etc.)</li>
   * <li>Are immutable or have controlled mutability</li>
   * </ul>
   *
   * @param allowedClassNames exact fully qualified class names to allow (e.g.,
   *        "com.example.MyClass")
   * @return configured filter with extended whitelist
   */
  public static SafeDeserializationFilter createWithAllowedClasses(String... allowedClassNames) {
    Set<String> allowedClasses = new HashSet<>();
    for (String className : allowedClassNames) {
      allowedClasses.add(className);
    }
    return new SafeDeserializationFilter(allowedClasses, new HashSet<>());
  }

  /**
   * Factory method to create a filter with additional allowed class patterns.
   *
   * <p>
   * Use this when you need to allow multiple related classes using regex patterns.
   * More flexible than {@link #createWithAllowedClasses(String...)} but also more risky.
   *
   * <p>
   * <b>Example patterns:</b>
   *
   * <pre>
   * // Allow all classes in a package
   * createWithAllowedPatterns("^com\\.example\\.myapp\\.model\\..*")
   *
   * // Allow specific class suffixes
   * createWithAllowedPatterns("^com\\.example\\..*DTO$")
   * </pre>
   *
   * <p>
   * <b>Security Warning:</b> Regex patterns can be dangerous if too broad.
   * Avoid patterns like:
   * <ul>
   * <li>".*" - allows everything (defeats the purpose)</li>
   * <li>"^java\\..*" - could allow dangerous JDK internals</li>
   * <li>"^org\\..*" - could allow dangerous third-party libraries</li>
   * </ul>
   *
   * @param allowedPatterns regex patterns for allowed class names (Java regex syntax)
   * @return configured filter with extended pattern whitelist
   */
  public static SafeDeserializationFilter createWithAllowedPatterns(String... allowedPatterns) {
    Set<Pattern> patterns = new HashSet<>();
    for (String pattern : allowedPatterns) {
      patterns.add(Pattern.compile(pattern));
    }
    return new SafeDeserializationFilter(new HashSet<>(), patterns);
  }
}
