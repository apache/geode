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
 * <p>
 * <b>Architecture - Dual Allowlist Structure:</b><br>
 * This filter uses a two-tier approach for optimal performance and flexibility:
 * <ul>
 * <li><b>ALLOWED_CLASSES (Set&lt;String&gt;):</b> Exact class names for O(1) fast lookup<br>
 * Examples: "java.lang.String", "java.util.HashMap"<br>
 * Use for: Known exact class names (10-100x faster than patterns)</li>
 *
 * <li><b>ALLOWED_PATTERNS (Set&lt;Pattern&gt;):</b> Regex patterns for flexible matching<br>
 * Examples: "^java\\.time\\..*" (all java.time classes), "^java\\.util\\.Collections\\$.*"<br>
 * Use for: Package wildcards and inner class patterns</li>
 * </ul>
 *
 * <p>
 * <b>Architecture - Dual Blocklist Structure:</b><br>
 * Similarly, blocking uses two collections for consistency:
 * <ul>
 * <li><b>BLOCKED_CLASSES (Set&lt;String&gt;):</b> Known dangerous classes (InvokerTransformer,
 * TemplatesImpl, etc.)</li>
 * <li><b>BLOCKED_PATTERNS (Set&lt;Pattern&gt;):</b> Dangerous package patterns
 * (org.apache.commons.collections.functors.*,
 * etc.)</li>
 * </ul>
 *
 * <p>
 * <b>Performance Characteristics:</b>
 * <table border="1" cellpadding="5" cellspacing="0">
 * <tr>
 * <th>Operation</th>
 * <th>Time Complexity</th>
 * <th>Typical Time</th>
 * </tr>
 * <tr>
 * <td>Exact match (ALLOWED_CLASSES)</td>
 * <td>O(1)</td>
 * <td>50-100 ns</td>
 * </tr>
 * <tr>
 * <td>Pattern match (ALLOWED_PATTERNS)</td>
 * <td>O(n)</td>
 * <td>500-1000 ns</td>
 * </tr>
 * </table>
 * <p>
 * For high-throughput systems deserializing thousands of objects per second, the exact match
 * optimization provides measurable CPU savings.
 *
 * <p>
 * <b>Usage Examples:</b>
 *
 * <pre>
 * // Default filter (uses built-in allowlist)
 * SafeDeserializationFilter filter = new SafeDeserializationFilter();
 *
 * // Custom exact classes (fast)
 * SafeDeserializationFilter filter =
 *     SafeDeserializationFilter.createWithAllowedClasses("com.example.User", "com.example.Order");
 *
 * // Custom patterns (flexible)
 * SafeDeserializationFilter filter =
 *     SafeDeserializationFilter.createWithAllowedPatterns("^com\\.example\\.dto\\..*");
 * </pre>
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
   * Exact class names allowed for deserialization (whitelist).
   *
   * <p>
   * This set contains specific fully-qualified class names that are safe to deserialize.
   * Classes in this set are checked using O(1) HashSet lookup, which is 10-100x faster
   * than regex pattern matching.
   *
   * <p>
   * <b>Performance Optimization:</b><br>
   * For frequently deserialized classes like String, Integer, HashMap, etc., exact matching
   * provides significant performance benefits over regex patterns. This is especially important
   * in high-throughput systems that deserialize thousands of objects per second.
   *
   * <p>
   * <b>When to use ALLOWED_CLASSES vs ALLOWED_PATTERNS:</b>
   * <ul>
   * <li><b>Use ALLOWED_CLASSES</b> when you know the exact class name (e.g.,
   * "java.lang.String")</li>
   * <li><b>Use ALLOWED_PATTERNS</b> when you need wildcards (e.g., "java.time.*" to allow all
   * java.time classes)</li>
   * </ul>
   *
   * <p>
   * <b>Architecture Consistency:</b><br>
   * This mirrors the BLOCKED structure which also uses separate collections for exact matches
   * (BLOCKED_CLASSES) and patterns (BLOCKED_PATTERNS), providing consistency throughout the
   * codebase.
   *
   * @see #ALLOWED_PATTERNS for wildcard/regex-based matching
   * @see #BLOCKED_CLASSES for the equivalent blocklist structure
   */
  private static final Set<String> ALLOWED_CLASSES = new HashSet<>();

  /**
   * Regex patterns for allowed class names (whitelist).
   *
   * <p>
   * This set contains regex patterns for flexible class name matching, particularly useful for:
   * <ul>
   * <li>Package-level wildcards (e.g., "^java\\.time\\..*" allows all java.time.* classes)</li>
   * <li>Inner class patterns (e.g., "^java\\.util\\.Collections\\$.*" allows
   * Collections.UnmodifiableList, etc.)</li>
   * <li>Dynamic class hierarchies where exact names aren't known in advance</li>
   * </ul>
   *
   * <p>
   * <b>Performance Note:</b><br>
   * Pattern matching is O(n) where n is the number of patterns, and each regex match is slower
   * than a simple string comparison. For exact class names, use ALLOWED_CLASSES instead.
   *
   * @see #ALLOWED_CLASSES for exact class name matching (faster)
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

    // ===== EXACT CLASS MATCHES (ALLOWED_CLASSES) =====
    // These use O(1) HashSet lookup for optimal performance

    // Allow safe primitive wrappers
    // These are immutable, have no dangerous methods, and are used extensively in session storage
    ALLOWED_CLASSES.add("java.lang.String");
    ALLOWED_CLASSES.add("java.lang.Number");
    ALLOWED_CLASSES.add("java.lang.Integer");
    ALLOWED_CLASSES.add("java.lang.Long");
    ALLOWED_CLASSES.add("java.lang.Float");
    ALLOWED_CLASSES.add("java.lang.Double");
    ALLOWED_CLASSES.add("java.lang.Boolean");
    ALLOWED_CLASSES.add("java.lang.Byte");
    ALLOWED_CLASSES.add("java.lang.Short");
    ALLOWED_CLASSES.add("java.lang.Character");
    ALLOWED_CLASSES.add("java.math.BigInteger");
    ALLOWED_CLASSES.add("java.math.BigDecimal");

    // Allow safe collection classes
    // These standard Java collections are widely used for session data storage
    ALLOWED_CLASSES.add("java.util.ArrayList");
    ALLOWED_CLASSES.add("java.util.LinkedList");
    ALLOWED_CLASSES.add("java.util.HashMap");
    ALLOWED_CLASSES.add("java.util.LinkedHashMap");
    ALLOWED_CLASSES.add("java.util.TreeMap");
    ALLOWED_CLASSES.add("java.util.HashSet");
    ALLOWED_CLASSES.add("java.util.LinkedHashSet");
    ALLOWED_CLASSES.add("java.util.TreeSet");
    ALLOWED_CLASSES.add("java.util.concurrent.ConcurrentHashMap");

    // Allow date/time classes
    // Common for storing timestamps, dates in session attributes
    ALLOWED_CLASSES.add("java.util.Date");
    ALLOWED_CLASSES.add("java.sql.Date");
    ALLOWED_CLASSES.add("java.sql.Time");
    ALLOWED_CLASSES.add("java.sql.Timestamp");

    // Allow UUID - Safe immutable identifier class
    // Used extensively in Geode for session IDs, disk stores, cache keys
    ALLOWED_CLASSES.add("java.util.UUID");

    // Allow Optional classes - Safe immutable wrapper classes (Java 8+)
    // Used in modern Java APIs for null-safety
    ALLOWED_CLASSES.add("java.util.Optional");
    ALLOWED_CLASSES.add("java.util.OptionalInt");
    ALLOWED_CLASSES.add("java.util.OptionalLong");
    ALLOWED_CLASSES.add("java.util.OptionalDouble");

    // Allow Locale - Safe immutable internationalization/localization class
    // Used for storing user's language/region preferences in sessions
    ALLOWED_CLASSES.add("java.util.Locale");

    // Allow URI - Safe immutable resource identifier (safer than URL)
    // Used for redirect URLs, resource identifiers in session data
    // Note: URI is preferred over URL because URL performs DNS lookups in equals()/hashCode()
    ALLOWED_CLASSES.add("java.net.URI");

    // Allow arrays of primitives and safe classes
    // Arrays are commonly used in session storage for bulk data
    ALLOWED_CLASSES.add("[Z"); // boolean[]
    ALLOWED_CLASSES.add("[B"); // byte[]
    ALLOWED_CLASSES.add("[C"); // char[]
    ALLOWED_CLASSES.add("[S"); // short[]
    ALLOWED_CLASSES.add("[I"); // int[]
    ALLOWED_CLASSES.add("[J"); // long[]
    ALLOWED_CLASSES.add("[F"); // float[]
    ALLOWED_CLASSES.add("[D"); // double[]
    ALLOWED_CLASSES.add("[Ljava.lang.String;"); // String[]
    ALLOWED_CLASSES.add("[Ljava.lang.Object;"); // Object[]

    // ===== PATTERN-BASED MATCHES (ALLOWED_PATTERNS) =====
    // These use regex matching for flexible package-level allowances

    // Allow java.time.* package (LocalDate, LocalDateTime, Instant, ZonedDateTime, etc.)
    // Modern Java date/time API - all classes are immutable and safe
    ALLOWED_PATTERNS.add(Pattern.compile("^java\\.time\\..*"));

    // Allow Collections inner classes (UnmodifiableList, SynchronizedMap, etc.)
    // These wrapper classes are safe as they delegate to underlying collections
    ALLOWED_PATTERNS.add(Pattern.compile("^java\\.util\\.Collections\\$.*"));
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
   * This method implements a two-tier whitelist check for optimal performance:
   * <ol>
   * <li><b>FAST PATH (O(1)):</b> Check exact matches in ALLOWED_CLASSES and custom allowed
   * classes using HashSet lookup</li>
   * <li><b>SLOW PATH (O(n)):</b> Check pattern matches in ALLOWED_PATTERNS and custom allowed
   * patterns using regex matching</li>
   * </ol>
   *
   * <p>
   * <b>Performance Optimization:</b><br>
   * The fast path is checked first because it's 10-100x faster than regex matching.
   * For frequently deserialized classes like String, Integer, HashMap, this provides
   * significant performance benefits:
   * <ul>
   * <li>Exact match (HashSet): ~50-100 nanoseconds</li>
   * <li>Pattern match (Regex): ~500-1000 nanoseconds</li>
   * </ul>
   *
   * <p>
   * <b>Example Flow:</b>
   *
   * <pre>
   * isClassAllowed("java.lang.String")
   *   → Check ALLOWED_CLASSES.contains("java.lang.String") → TRUE (50ns)
   *   → Return immediately without checking patterns
   *
   * isClassAllowed("java.time.Instant")
   *   → Check ALLOWED_CLASSES.contains("java.time.Instant") → FALSE (50ns)
   *   → Check customAllowedClasses.contains("java.time.Instant") → FALSE (50ns)
   *   → Check ALLOWED_PATTERNS (^java\.time\..*) → TRUE (500ns)
   *   → Return true
   * </pre>
   *
   * @param className fully qualified class name to check (e.g., "java.lang.String")
   * @return true if the class is whitelisted (either exact match or pattern match), false
   *         otherwise
   */
  private boolean isClassAllowed(String className) {
    // ===== FAST PATH: Check exact matches first (O(1) HashSet lookup) =====
    // This is 10-100x faster than regex matching and handles the most common cases

    // Check default exact matches (String, Integer, HashMap, etc.)
    if (ALLOWED_CLASSES.contains(className)) {
      return true;
    }

    // Check custom exact matches (application-specific classes)
    if (customAllowedClasses.contains(className)) {
      return true;
    }

    // ===== SLOW PATH: Check regex patterns (O(n) pattern matching) =====
    // Only reached if exact match fails - handles wildcards like java.time.*

    // Check default patterns (java.time.*, Collections$*)
    for (Pattern pattern : ALLOWED_PATTERNS) {
      if (pattern.matcher(className).matches()) {
        return true;
      }
    }

    // Check custom patterns (application-specific patterns)
    for (Pattern pattern : customAllowedPatterns) {
      if (pattern.matcher(className).matches()) {
        return true;
      }
    }

    // Not found in either exact matches or patterns - reject by default
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
