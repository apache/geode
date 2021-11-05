/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jboss.modules.xml;

import static org.jboss.modules.xml.XmlPullParser.CDSECT;
import static org.jboss.modules.xml.XmlPullParser.COMMENT;
import static org.jboss.modules.xml.XmlPullParser.DOCDECL;
import static org.jboss.modules.xml.XmlPullParser.END_DOCUMENT;
import static org.jboss.modules.xml.XmlPullParser.END_TAG;
import static org.jboss.modules.xml.XmlPullParser.ENTITY_REF;
import static org.jboss.modules.xml.XmlPullParser.FEATURE_PROCESS_NAMESPACES;
import static org.jboss.modules.xml.XmlPullParser.IGNORABLE_WHITESPACE;
import static org.jboss.modules.xml.XmlPullParser.PROCESSING_INSTRUCTION;
import static org.jboss.modules.xml.XmlPullParser.START_DOCUMENT;
import static org.jboss.modules.xml.XmlPullParser.START_TAG;
import static org.jboss.modules.xml.XmlPullParser.TEXT;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.jar.JarFile;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.jboss.modules.AssertionSetting;
import org.jboss.modules.DependencySpec;
import org.jboss.modules.LocalDependencySpecBuilder;
import org.jboss.modules.ModuleDependencySpec;
import org.jboss.modules.ModuleDependencySpecBuilder;
import org.jboss.modules.ModuleIdentifier;
import org.jboss.modules.ModuleLoadException;
import org.jboss.modules.ModuleLoader;
import org.jboss.modules.ModuleSpec;
import org.jboss.modules.NativeLibraryResourceLoader;
import org.jboss.modules.PathUtils;
import org.jboss.modules.ResourceLoader;
import org.jboss.modules.ResourceLoaderSpec;
import org.jboss.modules.ResourceLoaders;
import org.jboss.modules.Version;
import org.jboss.modules.VersionDetection;
import org.jboss.modules.filter.MultiplePathFilterBuilder;
import org.jboss.modules.filter.PathFilter;
import org.jboss.modules.filter.PathFilters;
import org.jboss.modules.maven.ArtifactCoordinates;
import org.jboss.modules.maven.MavenResolver;
import org.jboss.modules.security.FactoryPermissionCollection;
import org.jboss.modules.security.ModularPermissionFactory;
import org.jboss.modules.security.PermissionFactory;

public class GeodeModuleXmlParser {

  static final ModuleDependencySpec DEP_JAVA_SE =
      new ModuleDependencySpecBuilder().setName("java.se").build();
  static final ModuleDependencySpec DEP_JDK_UNSUPPORTED =
      new ModuleDependencySpecBuilder().setName("jdk.unsupported").setOptional(true).build();

  /**
   * A factory for resource roots, based on a root path, loader path, and loader name. Normally it
   * is sufficient to
   * accept the default.
   */
  public interface ResourceRootFactory {
    ResourceLoader createResourceLoader(final String rootPath, final String loaderPath,
        @Deprecated final String loaderName) throws IOException;

    /**
     * Get the default resource root factory.
     *
     * @return the default resource root factory (not {@code null})
     */
    static GeodeModuleXmlParser.ResourceRootFactory getDefault() {
      return GeodeModuleXmlParser.DefaultResourceRootFactory.INSTANCE;
    }
  }

  /**
   * XML parsing callback for property elements.
   */
  private interface PropertiesCallback {
    void parsedProperty(String name, String value);
  }

  private GeodeModuleXmlParser() {}

  private static final String MODULE_1_0 = "urn:jboss:module:1.0";
  private static final String MODULE_1_1 = "urn:jboss:module:1.1";
  private static final String MODULE_1_2 = "urn:jboss:module:1.2";
  private static final String MODULE_1_3 = "urn:jboss:module:1.3";
  // there is no 1.4
  private static final String MODULE_1_5 = "urn:jboss:module:1.5";
  private static final String MODULE_1_6 = "urn:jboss:module:1.6";
  private static final String MODULE_1_7 = "urn:jboss:module:1.7";
  private static final String MODULE_1_8 = "urn:jboss:module:1.8";
  private static final String MODULE_1_9 = "urn:jboss:module:1.9";

  private static final String E_MODULE = "module";
  private static final String E_ARTIFACT = "artifact";
  private static final String E_NATIVE_ARTIFACT = "native-artifact";
  private static final String E_DEPENDENCIES = "dependencies";
  private static final String E_RESOURCES = "resources";
  private static final String E_MAIN_CLASS = "main-class";
  private static final String E_RESOURCE_ROOT = "resource-root";
  private static final String E_PATH = "path";
  private static final String E_EXPORTS = "exports";
  private static final String E_IMPORTS = "imports";
  private static final String E_INCLUDE = "include";
  private static final String E_EXCLUDE = "exclude";
  private static final String E_INCLUDE_SET = "include-set";
  private static final String E_EXCLUDE_SET = "exclude-set";
  private static final String E_FILTER = "filter";
  private static final String E_SYSTEM = "system";
  private static final String E_PATHS = "paths";
  private static final String E_MODULE_ALIAS = "module-alias";
  private static final String E_MODULE_ABSENT = "module-absent";
  private static final String E_PROPERTIES = "properties";
  private static final String E_PROPERTY = "property";
  private static final String E_PERMISSIONS = "permissions";
  private static final String E_GRANT = "grant";
  private static final String E_CONDITIONS = "conditions";
  private static final String E_PROPERTY_EQUAL = "property-equal";
  private static final String E_PROPERTY_NOT_EQUAL = "property-not-equal";
  private static final String E_PROVIDES = "provides";
  private static final String E_SERVICE = "service";
  private static final String E_WITH_CLASS = "with-class";

  private static final String A_NAME = "name";
  private static final String A_SLOT = "slot";
  private static final String A_EXPORT = "export";
  private static final String A_SERVICES = "services";
  private static final String A_PATH = "path";
  private static final String A_OPTIONAL = "optional";
  private static final String A_TARGET_NAME = "target-name";
  private static final String A_TARGET_SLOT = "target-slot";
  private static final String A_VALUE = "value";
  private static final String A_PERMISSION = "permission";
  private static final String A_ACTIONS = "actions";
  private static final String A_VERSION = "version";

  private static final String D_NONE = "none";
  private static final String D_IMPORT = "import";
  private static final String D_EXPORT = "export";

  private static final List<String> LIST_A_NAME = Collections.singletonList(A_NAME);
  private static final List<String> LIST_A_PATH = Collections.singletonList(A_PATH);

  private static final List<String> LIST_A_NAME_A_SLOT = Arrays.asList(A_NAME, A_SLOT);
  private static final List<String> LIST_A_NAME_A_TARGET_NAME =
      Arrays.asList(A_NAME, A_TARGET_NAME);
  private static final List<String> LIST_A_PERMISSION_A_NAME = Arrays.asList(A_PERMISSION, A_NAME);

  private static GeodeModuleXmlParser.PropertiesCallback NOOP_PROPS_CALLBACK;

  /**
   * Returns a no-op implementation of properties callback currently used for
   * parsing properties attached to module dependencies that aren't used at
   * runtime in jboss-modules but are important in other projects where
   * module dependencies are analyzed.
   *
   * @return a no-op implementation of properties callback
   */
  private static GeodeModuleXmlParser.PropertiesCallback getNoopPropsCallback() {
    return NOOP_PROPS_CALLBACK == null
        ? NOOP_PROPS_CALLBACK = new GeodeModuleXmlParser.PropertiesCallback() {
          @Override
          public void parsedProperty(String name, String value) {}
        }
        : NOOP_PROPS_CALLBACK;
  }

  /**
   * Parse a {@code module.xml} file.
   *
   * @param moduleLoader the module loader to use for dependency specifications
   * @param moduleIdentifier the module identifier of the module to load
   * @param root the module path root
   * @param moduleInfoFile the {@code File} of the {@code module.xml} content
   * @return a module specification
   * @throws ModuleLoadException if a dependency could not be established or another error occurs
   * @throws IOException if I/O fails
   * @deprecated Use {@link #parseModuleXml(ModuleLoader, String, File, File)} instead.
   */
  @Deprecated
  public static ModuleSpec parseModuleXml(final ModuleLoader moduleLoader,
      final ModuleIdentifier moduleIdentifier, final File root, final File moduleInfoFile)
      throws ModuleLoadException, IOException {
    return parseModuleXml(moduleLoader, moduleIdentifier.toString(), root, moduleInfoFile);
  }

  /**
   * Parse a {@code module.xml} file.
   *
   * @param moduleLoader the module loader to use for dependency specifications
   * @param moduleName the name of the module to load
   * @param root the module path root
   * @param moduleInfoFile the {@code File} of the {@code module.xml} content
   * @return a module specification
   * @throws ModuleLoadException if a dependency could not be established or another error occurs
   * @throws IOException if I/O fails
   */
  public static ModuleSpec parseModuleXml(final ModuleLoader moduleLoader, final String moduleName,
      final File root, final File moduleInfoFile) throws ModuleLoadException, IOException {
    return parseModuleXml(GeodeModuleXmlParser.ResourceRootFactory.getDefault(), moduleLoader,
        moduleName, root, moduleInfoFile);
  }

  /**
   * Parse a {@code module.xml} file.
   *
   * @param factory the resource root factory to use (must not be {@code null})
   * @param moduleLoader the module loader to use for dependency specifications
   * @param moduleName the name of the module to load
   * @param root the module path root
   * @param moduleInfoFile the {@code File} of the {@code module.xml} content
   * @return a module specification
   * @throws ModuleLoadException if a dependency could not be established or another error occurs
   * @throws IOException if I/O fails
   */
  public static ModuleSpec parseModuleXml(final GeodeModuleXmlParser.ResourceRootFactory factory,
      final ModuleLoader moduleLoader, final String moduleName, final File root,
      final File moduleInfoFile) throws ModuleLoadException, IOException {
    final FileInputStream fis;
    try {
      fis = new FileInputStream(moduleInfoFile);
    } catch (FileNotFoundException e) {
      throw new ModuleLoadException("No module.xml file found at " + moduleInfoFile);
    }
    try {
      return parseModuleXml(factory, root.getPath(), new BufferedInputStream(fis),
          moduleInfoFile.getPath(), moduleLoader, moduleName);
    } finally {
      safeClose(fis);
    }
  }

  /**
   * Parse a {@code module.xml} file.
   *
   * @param factory the resource root factory to use (must not be {@code null})
   * @param rootPath the root path to send in to the resource root factory (must not be
   *        {@code null})
   * @param source a stream of the {@code module.xml} content (must not be {@code null})
   * @param moduleInfoFile the {@code File} of the {@code module.xml} content (must not be
   *        {@code null})
   * @param moduleLoader the module loader to use for dependency specifications (must not be
   *        {@code null})
   * @param moduleIdentifier the module identifier of the module to load
   * @return a module specification
   * @throws ModuleLoadException if a dependency could not be established or another error occurs
   * @throws IOException if I/O fails
   * @deprecated Use
   *             {@link #parseModuleXml(GeodeModuleXmlParser.ResourceRootFactory, String, InputStream, String, ModuleLoader, String)}
   *             instead.
   */
  @Deprecated
  public static ModuleSpec parseModuleXml(final GeodeModuleXmlParser.ResourceRootFactory factory,
      final String rootPath, InputStream source, final String moduleInfoFile,
      final ModuleLoader moduleLoader, final ModuleIdentifier moduleIdentifier)
      throws ModuleLoadException, IOException {
    return parseModuleXml(factory, MavenResolver.createDefaultResolver(), rootPath, source,
        moduleInfoFile, moduleLoader, moduleIdentifier);
  }

  /**
   * Parse a {@code module.xml} file.
   *
   * @param factory the resource root factory to use (must not be {@code null})
   * @param rootPath the root path to send in to the resource root factory (must not be
   *        {@code null})
   * @param source a stream of the {@code module.xml} content (must not be {@code null})
   * @param moduleInfoFile the {@code File} of the {@code module.xml} content (must not be
   *        {@code null})
   * @param moduleLoader the module loader to use for dependency specifications (must not be
   *        {@code null})
   * @param moduleName the module name of the module to load
   * @return a module specification
   * @throws ModuleLoadException if a dependency could not be established or another error occurs
   * @throws IOException if I/O fails
   */
  public static ModuleSpec parseModuleXml(final GeodeModuleXmlParser.ResourceRootFactory factory,
      final String rootPath, InputStream source, final String moduleInfoFile,
      final ModuleLoader moduleLoader, final String moduleName)
      throws ModuleLoadException, IOException {
    return parseModuleXml(factory, MavenResolver.createDefaultResolver(), rootPath, source,
        moduleInfoFile, moduleLoader, moduleName);
  }

  /**
   * Parse a {@code module.xml} file.
   *
   * @param factory the resource root factory to use (must not be {@code null})
   * @param mavenResolver the Maven artifact resolver to use (must not be {@code null})
   * @param rootPath the root path to send in to the resource root factory (must not be
   *        {@code null})
   * @param source a stream of the {@code module.xml} content (must not be {@code null})
   * @param moduleInfoFile the {@code File} of the {@code module.xml} content (must not be
   *        {@code null})
   * @param moduleLoader the module loader to use for dependency specifications (must not be
   *        {@code null})
   * @param moduleIdentifier the module identifier of the module to load
   * @return a module specification
   * @throws ModuleLoadException if a dependency could not be established or another error occurs
   * @throws IOException if I/O fails
   * @deprecated Use
   *             {@link #parseModuleXml(GeodeModuleXmlParser.ResourceRootFactory, MavenResolver, String, InputStream, String, ModuleLoader, String)}
   *             instead.
   */
  @Deprecated
  public static ModuleSpec parseModuleXml(final GeodeModuleXmlParser.ResourceRootFactory factory,
      final MavenResolver mavenResolver, final String rootPath, InputStream source,
      final String moduleInfoFile, final ModuleLoader moduleLoader,
      final ModuleIdentifier moduleIdentifier) throws ModuleLoadException, IOException {
    return parseModuleXml(factory, mavenResolver, rootPath, source, moduleInfoFile, moduleLoader,
        moduleIdentifier.toString());
  }

  /**
   * Parse a {@code module.xml} file.
   *
   * @param factory the resource root factory to use (must not be {@code null})
   * @param mavenResolver the Maven artifact resolver to use (must not be {@code null})
   * @param rootPath the root path to send in to the resource root factory (must not be
   *        {@code null})
   * @param source a stream of the {@code module.xml} content (must not be {@code null})
   * @param moduleInfoFile the {@code File} of the {@code module.xml} content (must not be
   *        {@code null})
   * @param moduleLoader the module loader to use for dependency specifications (must not be
   *        {@code null})
   * @param moduleName the module name of the module to load
   * @return a module specification
   * @throws ModuleLoadException if a dependency could not be established or another error occurs
   * @throws IOException if I/O fails
   */
  public static ModuleSpec parseModuleXml(final GeodeModuleXmlParser.ResourceRootFactory factory,
      final MavenResolver mavenResolver, final String rootPath, InputStream source,
      final String moduleInfoFile, final ModuleLoader moduleLoader, final String moduleName)
      throws ModuleLoadException, IOException {
    try {
      final MXParser parser = new MXParser();
      parser.setFeature(FEATURE_PROCESS_NAMESPACES, true);
      parser.setInput(source, null);
      return parseDocument(mavenResolver, factory, rootPath, parser, moduleLoader, moduleName);
    } catch (XmlPullParserException e) {
      throw new ModuleLoadException("Error loading module from " + moduleInfoFile, e);
    } finally {
      safeClose(source);
    }
  }

  public static XmlPullParserException unexpectedContent(final XmlPullParser reader) {
    final String kind;
    switch (reader.getEventType()) {
      case CDSECT:
        kind = "cdata";
        break;
      case COMMENT:
        kind = "comment";
        break;
      case DOCDECL:
        kind = "document decl";
        break;
      case END_DOCUMENT:
        kind = "document end";
        break;
      case END_TAG:
        kind = "element end";
        break;
      case ENTITY_REF:
        kind = "entity ref";
        break;
      case PROCESSING_INSTRUCTION:
        kind = "processing instruction";
        break;
      case IGNORABLE_WHITESPACE:
        kind = "whitespace";
        break;
      case START_DOCUMENT:
        kind = "document start";
        break;
      case START_TAG:
        kind = "element start";
        break;
      case TEXT:
        kind = "text";
        break;
      default:
        kind = "unknown";
        break;
    }
    final StringBuilder b =
        new StringBuilder("Unexpected content of type '").append(kind).append('\'');
    if (reader.getName() != null) {
      b.append(" named '").append(reader.getName()).append('\'');
    }
    if (reader.getText() != null) {
      b.append(", text is: '").append(reader.getText()).append('\'');
    }
    return new XmlPullParserException(b.toString(), reader, null);
  }

  public static XmlPullParserException endOfDocument(final XmlPullParser reader) {
    return new XmlPullParserException("Unexpected end of document", reader, null);
  }

  private static XmlPullParserException invalidModuleName(final XmlPullParser reader,
      final String expected) {
    return new XmlPullParserException("Invalid/mismatched module name (expected " + expected + ")",
        reader, null);
  }

  private static XmlPullParserException missingAttributes(final XmlPullParser reader,
      final Set<String> required) {
    final StringBuilder b = new StringBuilder("Missing one or more required attributes:");
    for (String attribute : required) {
      b.append(' ').append(attribute);
    }
    return new XmlPullParserException(b.toString(), reader, null);
  }

  static XmlPullParserException unknownAttribute(final XmlPullParser parser, final int index) {
    final String namespace = parser.getAttributeNamespace(index);
    final String prefix = parser.getAttributePrefix(index);
    final String name = parser.getAttributeName(index);
    final StringBuilder eb = new StringBuilder("Unknown attribute \"");
    if (prefix != null)
      eb.append(prefix).append(':');
    eb.append(name);
    if (namespace != null)
      eb.append("\" from namespace \"").append(namespace);
    eb.append('"');
    return new XmlPullParserException(eb.toString(), parser, null);
  }

  private static XmlPullParserException unknownAttributeValue(final XmlPullParser parser,
      final int index) {
    final String namespace = parser.getAttributeNamespace(index);
    final String prefix = parser.getAttributePrefix(index);
    final String name = parser.getAttributeName(index);
    final StringBuilder eb = new StringBuilder("Unknown value \"");
    eb.append(parser.getAttributeValue(index));
    eb.append("\" for attribute \"");
    if (prefix != null && !prefix.isEmpty())
      eb.append(prefix).append(':');
    eb.append(name);
    if (namespace != null && !namespace.isEmpty())
      eb.append("\" from namespace \"").append(namespace);
    eb.append('"');
    return new XmlPullParserException(eb.toString(), parser, null);
  }

  private static void validateNamespace(final XmlPullParser reader) throws XmlPullParserException {
    switch (reader.getNamespace()) {
      case MODULE_1_0:
      case MODULE_1_1:
      case MODULE_1_2:
      case MODULE_1_3:
      case MODULE_1_5:
      case MODULE_1_6:
      case MODULE_1_7:
      case MODULE_1_8:
      case MODULE_1_9:
        break;
      default:
        throw unexpectedContent(reader);
    }
  }

  private static boolean atLeast1_6(final XmlPullParser reader) {
    return MODULE_1_6.equals(reader.getNamespace()) || atLeast1_7(reader);
  }

  private static boolean atLeast1_7(final XmlPullParser reader) {
    return MODULE_1_7.equals(reader.getNamespace()) || atLeast1_8(reader);
  }

  private static boolean atLeast1_8(final XmlPullParser reader) {
    return MODULE_1_8.equals(reader.getNamespace()) || atLeast1_9(reader);
  }

  private static boolean atLeast1_9(final XmlPullParser reader) {
    return MODULE_1_9.equals(reader.getNamespace());
  }

  private static void assertNoAttributes(final XmlPullParser reader) throws XmlPullParserException {
    final int attributeCount = reader.getAttributeCount();
    if (attributeCount > 0) {
      throw unknownAttribute(reader, 0);
    }
  }

  private static void validateAttributeNamespace(final XmlPullParser reader, final int index)
      throws XmlPullParserException {
    if (!reader.getAttributeNamespace(index).isEmpty()) {
      throw unknownAttribute(reader, index);
    }
  }

  private static ModuleSpec parseDocument(final MavenResolver mavenResolver,
      final GeodeModuleXmlParser.ResourceRootFactory factory, final String rootPath,
      XmlPullParser reader, final ModuleLoader moduleLoader, final String moduleName)
      throws XmlPullParserException, IOException {
    int eventType;
    for (;;) {
      eventType = reader.nextTag();
      switch (eventType) {
        case START_DOCUMENT: {
          return parseRootElement(mavenResolver, factory, rootPath, reader, moduleLoader,
              moduleName);
        }
        case START_TAG: {
          validateNamespace(reader);
          final String element = reader.getName();
          switch (element) {
            case E_MODULE: {
              final ModuleSpec.Builder specBuilder = parseModuleContents(mavenResolver, reader,
                  factory, moduleLoader, moduleName, rootPath);
              parseEndDocument(reader);
              return specBuilder.create();
            }
            case E_MODULE_ALIAS: {
              final ModuleSpec moduleSpec = parseModuleAliasContents(reader, moduleName);
              parseEndDocument(reader);
              return moduleSpec;
            }
            case E_MODULE_ABSENT: {
              parseModuleAbsentContents(reader, moduleName);
              return null;
            }
            default: {
              throw unexpectedContent(reader);
            }
          }
        }
        default: {
          throw unexpectedContent(reader);
        }
      }
    }
  }

  private static ModuleSpec parseRootElement(final MavenResolver mavenResolver,
      final GeodeModuleXmlParser.ResourceRootFactory factory, final String rootPath,
      final XmlPullParser reader, final ModuleLoader moduleLoader, final String moduleName)
      throws XmlPullParserException, IOException {
    assertNoAttributes(reader);
    int eventType;
    while ((eventType = reader.nextTag()) != END_DOCUMENT) {
      switch (eventType) {
        case START_TAG: {
          validateNamespace(reader);
          final String element = reader.getName();
          switch (element) {
            case E_MODULE: {
              final ModuleSpec.Builder specBuilder = parseModuleContents(mavenResolver, reader,
                  factory, moduleLoader, moduleName, rootPath);
              parseEndDocument(reader);
              return specBuilder.create();
            }
            case E_MODULE_ALIAS: {
              final ModuleSpec moduleSpec = parseModuleAliasContents(reader, moduleName);
              parseEndDocument(reader);
              return moduleSpec;
            }
            case E_MODULE_ABSENT: {
              parseModuleAbsentContents(reader, moduleName);
              return null;
            }
            default: {
              throw unexpectedContent(reader);
            }
          }
        }
        default: {
          throw unexpectedContent(reader);
        }
      }
    }
    throw endOfDocument(reader);
  }

  private static ModuleSpec parseModuleAliasContents(final XmlPullParser reader,
      final String moduleName) throws XmlPullParserException, IOException {
    final int count = reader.getAttributeCount();
    String name = null;
    String slot = null;
    String targetName = null;
    String targetSlot = null;
    boolean noSlots = atLeast1_6(reader);
    final Set<String> required = new HashSet<>(LIST_A_NAME_A_TARGET_NAME);
    for (int i = 0; i < count; i++) {
      validateAttributeNamespace(reader, i);
      final String attribute = reader.getAttributeName(i);
      required.remove(attribute);
      switch (attribute) {
        case A_NAME:
          name = reader.getAttributeValue(i);
          break;
        case A_SLOT:
          if (noSlots)
            throw unknownAttribute(reader, i);
          else
            slot = reader.getAttributeValue(i);
          break;
        case A_TARGET_NAME:
          targetName = reader.getAttributeValue(i);
          break;
        case A_TARGET_SLOT:
          if (noSlots)
            throw unknownAttribute(reader, i);
          else
            targetSlot = reader.getAttributeValue(i);
          break;
        default:
          throw unknownAttribute(reader, i);
      }
    }
    if (!required.isEmpty()) {
      throw missingAttributes(reader, required);
    }
    if (moduleName != null) {
      if (noSlots) {
        if (!moduleName.equals(name)) {
          throw invalidModuleName(reader, moduleName);
        }
      } else {
        if (!ModuleIdentifier.fromString(moduleName).equals(ModuleIdentifier.create(name, slot))) {
          throw invalidModuleName(reader, moduleName);
        }
      }
    }
    int eventType;
    for (;;) {
      eventType = reader.nextTag();
      switch (eventType) {
        case END_TAG: {
          if (noSlots) {
            return ModuleSpec.buildAlias(name, targetName).create();
          } else {
            return ModuleSpec.buildAlias(ModuleIdentifier.create(name, slot),
                ModuleIdentifier.create(targetName, targetSlot)).create();
          }
        }
        default: {
          throw unexpectedContent(reader);
        }
      }
    }
  }

  private static void parseModuleAbsentContents(final XmlPullParser reader, final String moduleName)
      throws XmlPullParserException, IOException {
    final int count = reader.getAttributeCount();
    String name = null;
    String slot = null;
    boolean noSlots = atLeast1_6(reader);
    final Set<String> required =
        noSlots ? new HashSet<>(LIST_A_NAME) : new HashSet<>(LIST_A_NAME_A_SLOT);
    for (int i = 0; i < count; i++) {
      validateAttributeNamespace(reader, i);
      final String attribute = reader.getAttributeName(i);
      required.remove(attribute);
      switch (attribute) {
        case A_NAME:
          name = reader.getAttributeValue(i);
          break;
        case A_SLOT:
          if (noSlots)
            throw unknownAttribute(reader, i);
          else
            slot = reader.getAttributeValue(i);
          break;
        default:
          throw unknownAttribute(reader, i);
      }
    }
    if (!required.isEmpty()) {
      throw missingAttributes(reader, required);
    }
    if (moduleName != null) {
      if (noSlots) {
        if (!name.equals(moduleName)) {
          throw invalidModuleName(reader, moduleName);
        }
      } else {
        if (!ModuleIdentifier.fromString(moduleName).equals(ModuleIdentifier.create(name, slot))) {
          throw invalidModuleName(reader, moduleName);
        }
      }
    }
    int eventType;
    for (;;) {
      eventType = reader.nextTag();
      switch (eventType) {
        case END_TAG: {
          return;
        }
        default: {
          throw unexpectedContent(reader);
        }
      }
    }
  }

  private static ModuleSpec.Builder parseModuleContents(final MavenResolver mavenResolver,
      final XmlPullParser reader, final GeodeModuleXmlParser.ResourceRootFactory factory,
      final ModuleLoader moduleLoader, final String moduleName, final String rootPath)
      throws XmlPullParserException, IOException {
    final int count = reader.getAttributeCount();
    String name = null;
    String slot = null;
    boolean is1_6 = atLeast1_6(reader);
    Version version = null;
    final Set<String> required = is1_6 ? new HashSet<>(LIST_A_NAME) : new HashSet<>(LIST_A_NAME);
    for (int i = 0; i < count; i++) {
      validateAttributeNamespace(reader, i);
      final String attribute = reader.getAttributeName(i);
      required.remove(attribute);
      switch (attribute) {
        case A_NAME:
          name = reader.getAttributeValue(i);
          break;
        case A_SLOT:
          if (is1_6)
            throw unknownAttribute(reader, i);
          else
            slot = reader.getAttributeValue(i);
          break;
        case A_VERSION:
          try {
            version = Version.parse(reader.getAttributeValue(i));
            break;
          } catch (IllegalArgumentException ex) {
            throw new XmlPullParserException(ex.getMessage(), reader, ex);
          }
        default:
          throw unknownAttribute(reader, i);
      }
    }
    if (!required.isEmpty()) {
      throw missingAttributes(reader, required);
    }
    final String realModuleName;
    if (is1_6) {
      realModuleName = name;
    } else {
      realModuleName = ModuleIdentifier.create(name, slot).toString();
    }
    if (moduleName != null && !realModuleName.equals(moduleName)) {
      throw invalidModuleName(reader, realModuleName);
    }
    final ModuleSpec.Builder specBuilder = ModuleSpec.build(realModuleName);
    specBuilder.setVersion(version);
    // xsd:all
    MultiplePathFilterBuilder exportsBuilder = PathFilters.multiplePathFilterBuilder(true);
    ArrayList<DependencySpec> dependencies = new ArrayList<>();
    final boolean is1_8 = atLeast1_8(reader);
    if (!is1_8) {
      // add default system dependencies
      specBuilder.addDependency(DEP_JAVA_SE);
      specBuilder.addDependency(DEP_JDK_UNSUPPORTED);
    }
    Set<String> visited = new HashSet<>();
    int eventType;
    for (;;) {
      eventType = reader.nextTag();
      switch (eventType) {
        case END_TAG: {
          specBuilder.addDependency(new LocalDependencySpecBuilder()
              .setExportFilter(exportsBuilder.create())
              .build());
          for (DependencySpec dependency : dependencies) {
            specBuilder.addDependency(dependency);
          }
          return specBuilder;
        }
        case START_TAG: {
          validateNamespace(reader);
          final String element = reader.getName();
          if (visited.contains(element)) {
            throw unexpectedContent(reader);
          }
          visited.add(element);
          switch (element) {
            case E_EXPORTS:
              parseFilterList(reader, exportsBuilder);
              break;
            case E_DEPENDENCIES:
              parseDependencies(reader, dependencies);
              break;
            case E_MAIN_CLASS:
              parseMainClass(reader, specBuilder);
              break;
            case E_RESOURCES:
              parseResources(mavenResolver, factory, rootPath, reader, specBuilder);
              break;
            case E_PROPERTIES:
              parseProperties(reader, new GeodeModuleXmlParser.PropertiesCallback() {
                @Override
                public void parsedProperty(String name, String value) {
                  specBuilder.addProperty(name, value);
                  if ("jboss.assertions".equals(name))
                    try {
                      specBuilder.setAssertionSetting(
                          AssertionSetting.valueOf(value.toUpperCase(Locale.US)));
                    } catch (IllegalArgumentException ignored) {
                    }
                }
              });
              break;
            case E_PERMISSIONS:
              parsePermissions(reader, moduleLoader, realModuleName, specBuilder);
              break;
            case E_PROVIDES:
              if (is1_8)
                parseProvidesType(reader, specBuilder);
              else
                throw unexpectedContent(reader);
              break;
            default:
              throw unexpectedContent(reader);
          }
          break;
        }
        default: {
          throw unexpectedContent(reader);
        }
      }
    }
  }

  private static void parseDependencies(final XmlPullParser reader,
      final ArrayList<DependencySpec> dependencies) throws XmlPullParserException, IOException {
    assertNoAttributes(reader);
    // xsd:choice
    int eventType;
    for (;;) {
      eventType = reader.nextTag();
      switch (eventType) {
        case END_TAG: {
          return;
        }
        case START_TAG: {
          validateNamespace(reader);
          switch (reader.getName()) {
            case E_MODULE:
              parseModuleDependency(reader, dependencies);
              break;
            case E_SYSTEM:
              if (!atLeast1_8(reader)) {
                parseSystemDependency(reader, dependencies);
                break;
              }
            default:
              throw unexpectedContent(reader);
          }
          break;
        }
        default: {
          throw unexpectedContent(reader);
        }
      }
    }
  }

  private static void parseModuleDependency(final XmlPullParser reader,
      final ArrayList<DependencySpec> dependencies) throws XmlPullParserException, IOException {
    String name = null;
    String slot = null;
    boolean export = false;
    boolean optional = false;
    boolean noSlots = atLeast1_6(reader);
    String services = D_NONE;
    final Set<String> required = new HashSet<>(LIST_A_NAME);
    final int count = reader.getAttributeCount();
    for (int i = 0; i < count; i++) {
      validateAttributeNamespace(reader, i);
      final String attribute = reader.getAttributeName(i);
      required.remove(attribute);
      switch (attribute) {
        case A_NAME:
          name = reader.getAttributeValue(i);
          break;
        case A_SLOT:
          if (noSlots)
            throw unknownAttribute(reader, i);
          else
            slot = reader.getAttributeValue(i);
          break;
        case A_EXPORT:
          export = Boolean.parseBoolean(reader.getAttributeValue(i));
          break;
        case A_OPTIONAL:
          optional = Boolean.parseBoolean(reader.getAttributeValue(i));
          break;
        case A_SERVICES: {
          services = reader.getAttributeValue(i);
          switch (services) {
            case D_NONE:
            case D_IMPORT:
            case D_EXPORT:
              break;
            default:
              throw unknownAttributeValue(reader, i);
          }
          break;
        }
        default:
          throw unknownAttribute(reader, i);
      }
    }
    if (!required.isEmpty()) {
      throw missingAttributes(reader, required);
    }
    final MultiplePathFilterBuilder importBuilder = PathFilters.multiplePathFilterBuilder(true);
    final MultiplePathFilterBuilder exportBuilder = PathFilters.multiplePathFilterBuilder(export);
    int eventType;
    for (;;) {
      eventType = reader.nextTag();
      switch (eventType) {
        case END_TAG: {
          assert services.equals(D_NONE) || services.equals(D_EXPORT) || services.equals(D_IMPORT);
          if (services.equals(D_EXPORT)) {
            // If services are to be re-exported, add META-INF/services -> true near the end of the
            // list
            exportBuilder.addFilter(PathFilters.getMetaInfServicesFilter(), true);
          }
          if (export) {
            // If re-exported, add META-INF/** -> false at the end of the list (require explicit
            // override)
            exportBuilder.addFilter(PathFilters.getMetaInfSubdirectoriesFilter(), false);
            exportBuilder.addFilter(PathFilters.getMetaInfFilter(), false);
          }
          final PathFilter exportFilter = exportBuilder.create();
          final PathFilter importFilter;
          if (importBuilder.isEmpty()) {
            importFilter = services.equals(D_NONE) ? PathFilters.getDefaultImportFilter()
                : PathFilters.getDefaultImportFilterWithServices();
          } else {
            if (!services.equals(D_NONE)) {
              importBuilder.addFilter(PathFilters.getMetaInfServicesFilter(), true);
            }
            importBuilder.addFilter(PathFilters.getMetaInfSubdirectoriesFilter(), false);
            importBuilder.addFilter(PathFilters.getMetaInfFilter(), false);
            importFilter = importBuilder.create();
          }
          dependencies.add(new ModuleDependencySpecBuilder()
              .setImportFilter(importFilter)
              .setExportFilter(exportFilter)
              .setName(noSlots ? name : ModuleIdentifier.create(name, slot).toString())
              .setOptional(optional)
              .build());
          return;
        }
        case START_TAG: {
          validateNamespace(reader);
          switch (reader.getName()) {
            case E_EXPORTS:
              parseFilterList(reader, exportBuilder);
              break;
            case E_IMPORTS:
              parseFilterList(reader, importBuilder);
              break;
            case E_PROPERTIES:
              if (atLeast1_9(reader)) {
                parseProperties(reader, getNoopPropsCallback());
                break;
              }
            default:
              throw unexpectedContent(reader);
          }
          break;
        }
        default: {
          throw unexpectedContent(reader);
        }
      }
    }
  }

  private static void parseSystemDependency(final XmlPullParser reader,
      final ArrayList<DependencySpec> dependencies) throws XmlPullParserException, IOException {
    boolean export = false;
    final int count = reader.getAttributeCount();
    for (int i = 0; i < count; i++) {
      validateAttributeNamespace(reader, i);
      final String attribute = reader.getAttributeName(i);
      switch (attribute) {
        case A_EXPORT:
          export = Boolean.parseBoolean(reader.getAttributeValue(i));
          break;
        default:
          throw unexpectedContent(reader);
      }
    }
    Set<String> paths = Collections.emptySet();
    final MultiplePathFilterBuilder exportBuilder = PathFilters.multiplePathFilterBuilder(export);
    int eventType;
    for (;;) {
      eventType = reader.nextTag();
      switch (eventType) {
        case END_TAG: {
          final PathFilter exportFilter = exportBuilder.create();
          dependencies.add(DependencySpec.createSystemDependencySpec(PathFilters.acceptAll(),
              exportFilter, paths));
          return;
        }
        case START_TAG: {
          validateNamespace(reader);
          switch (reader.getName()) {
            case E_PATHS: {
              paths = parseSet(reader);
              break;
            }
            case E_EXPORTS: {
              parseFilterList(reader, exportBuilder);
              break;
            }
            default: {
              throw unexpectedContent(reader);
            }
          }
        }
      }
    }
  }

  private static String parseClassNameType(final XmlPullParser reader)
      throws IOException, XmlPullParserException {
    String name = null;
    final Set<String> required = new HashSet<>(LIST_A_NAME);
    final int count = reader.getAttributeCount();
    for (int i = 0; i < count; i++) {
      validateAttributeNamespace(reader, i);
      final String attribute = reader.getAttributeName(i);
      required.remove(attribute);
      switch (attribute) {
        case A_NAME:
          name = reader.getAttributeValue(i);
          break;
        default:
          throw unexpectedContent(reader);
      }
    }
    if (!required.isEmpty()) {
      throw missingAttributes(reader, required);
    }
    // consume remainder of element
    parseNoContent(reader);
    return name;
  }

  private static void parseMainClass(final XmlPullParser reader,
      final ModuleSpec.Builder specBuilder) throws XmlPullParserException, IOException {
    specBuilder.setMainClass(parseClassNameType(reader));
  }

  private static void parseResources(final MavenResolver mavenResolver,
      final GeodeModuleXmlParser.ResourceRootFactory factory, final String rootPath,
      final XmlPullParser reader, final ModuleSpec.Builder specBuilder)
      throws XmlPullParserException, IOException {
    assertNoAttributes(reader);
    final List<Version> detectedVersions = new ArrayList<>();
    // xsd:choice
    int eventType;
    for (;;) {
      eventType = reader.nextTag();
      switch (eventType) {
        case END_TAG: {
          final Version specifiedVersion = specBuilder.getVersion();
          if (specifiedVersion == null) {
            final Iterator<Version> iterator = detectedVersions.iterator();
            if (iterator.hasNext()) {
              Version guess = iterator.next();
              while (iterator.hasNext()) {
                if (!guess.equals(iterator.next())) {
                  guess = null;
                  break;
                }
              }
              if (guess != null)
                specBuilder.setVersion(guess);
            }
          }
          specBuilder.addResourceRoot(ResourceLoaderSpec.createResourceLoaderSpec(
              new NativeLibraryResourceLoader(new File(rootPath, "lib")), PathFilters.rejectAll()));
          return;
        }
        case START_TAG: {
          validateNamespace(reader);
          switch (reader.getName()) {
            case E_RESOURCE_ROOT: {
              final Version version = parseResourceRoot(factory, rootPath, reader, specBuilder);
              if (version != null)
                detectedVersions.add(version);
              break;
            }
            case E_ARTIFACT: {
              final Version version = parseArtifact(factory, mavenResolver, reader, specBuilder);
              if (version != null)
                detectedVersions.add(version);
              break;
            }
            case E_NATIVE_ARTIFACT: {
              parseNativeArtifact(mavenResolver, reader, specBuilder);
              break;
            }
            default:
              throw unexpectedContent(reader);
          }
          break;
        }
        default: {
          throw unexpectedContent(reader);
        }
      }
    }
  }

  private static void createMavenNativeArtifactLoader(final MavenResolver mavenResolver,
      final String name, final XmlPullParser reader, final ModuleSpec.Builder specBuilder)
      throws IOException, XmlPullParserException {
    File fp = mavenResolver.resolveJarArtifact(ArtifactCoordinates.fromString(name));
    if (fp == null)
      throw new XmlPullParserException(
          String.format("Failed to resolve native artifact '%s'", name), reader, null);
    File lib = new File(fp.getParentFile(), "lib");
    if (!lib.exists()) {
      if (!fp.getParentFile().canWrite())
        throw new XmlPullParserException(
            String.format("Native artifact '%s' cannot be unpacked", name), reader, null);
      unzip(fp, fp.getParentFile());
    }
    specBuilder.addResourceRoot(ResourceLoaderSpec
        .createResourceLoaderSpec(new NativeLibraryResourceLoader(lib), PathFilters.rejectAll()));
  }

  private static void parseNativeArtifact(final MavenResolver mavenResolver,
      final XmlPullParser reader, final ModuleSpec.Builder specBuilder)
      throws XmlPullParserException, IOException {
    String name = null;
    final Set<String> required = new HashSet<>(LIST_A_NAME);
    final int count = reader.getAttributeCount();
    for (int i = 0; i < count; i++) {
      validateAttributeNamespace(reader, i);
      final String attribute = reader.getAttributeName(i);
      required.remove(attribute);
      switch (attribute) {
        case A_NAME:
          name = reader.getAttributeValue(i);
          break;
        default:
          throw unknownAttribute(reader, i);
      }
    }
    if (!required.isEmpty()) {
      throw missingAttributes(reader, required);
    }

    final SystemPropertyConditionBuilder conditionBuilder = new SystemPropertyConditionBuilder();
    final Set<String> encountered = new HashSet<>();
    int eventType;
    for (;;) {
      eventType = reader.nextTag();
      switch (eventType) {
        case END_TAG: {
          try {
            if (conditionBuilder.resolve()) {
              createMavenNativeArtifactLoader(mavenResolver, name, reader, specBuilder);
            }
          } catch (IOException e) {
            throw new XmlPullParserException(String.format("Failed to add artifact '%s'", name),
                reader, e);
          }
          return;
        }
        case START_TAG: {
          validateNamespace(reader);
          final String element = reader.getName();
          if (!encountered.add(element))
            throw unexpectedContent(reader);
          switch (element) {
            case E_CONDITIONS:
              parseConditions(reader, conditionBuilder);
              break;
            default:
              throw unexpectedContent(reader);
          }
          break;
        }
        default: {
          throw unexpectedContent(reader);
        }
      }
    }
  }

  private static Version parseArtifact(final GeodeModuleXmlParser.ResourceRootFactory factory,
      final MavenResolver mavenResolver, final XmlPullParser reader,
      final ModuleSpec.Builder specBuilder) throws XmlPullParserException, IOException {
    String name = null;
    final Set<String> required = new HashSet<>(LIST_A_NAME);
    final int count = reader.getAttributeCount();
    for (int i = 0; i < count; i++) {
      validateAttributeNamespace(reader, i);
      final String attribute = reader.getAttributeName(i);
      required.remove(attribute);
      switch (attribute) {
        case A_NAME:
          name = reader.getAttributeValue(i);
          break;
        default:
          throw unknownAttribute(reader, i);
      }
    }
    if (!required.isEmpty()) {
      throw missingAttributes(reader, required);
    }

    final MultiplePathFilterBuilder filterBuilder = PathFilters.multiplePathFilterBuilder(true);
    final SystemPropertyConditionBuilder conditionBuilder = new SystemPropertyConditionBuilder();
    final ResourceLoader resourceLoader;

    final Set<String> encountered = new HashSet<>();
    int eventType;
    for (;;) {
      eventType = reader.nextTag();
      switch (eventType) {
        case END_TAG: {
          if (conditionBuilder.resolve()) {
            final ArtifactCoordinates coordinates;
            try {
              coordinates = ArtifactCoordinates.fromString(name);
              final File file = mavenResolver.resolveJarArtifact(coordinates);
              if (file == null) {
                throw new XmlPullParserException(
                    String.format("Failed to resolve artifact '%s'", coordinates), reader, null);
              }
              resourceLoader = factory.createResourceLoader("", file.getPath(), name);
            } catch (IOException | IllegalArgumentException e) {
              throw new XmlPullParserException(String.format("Failed to add artifact '%s'", name),
                  reader, e);
            }
            if (resourceLoader == null)
              throw new XmlPullParserException(
                  String.format("Failed to resolve artifact '%s'", name), reader, null);
            specBuilder.addResourceRoot(ResourceLoaderSpec.createResourceLoaderSpec(resourceLoader,
                filterBuilder.create()));
            final String version = coordinates.getVersion();
            try {
              return Version.parse(version);
            } catch (IllegalArgumentException ignored) {
              return null;
            }
          }
          return null;
        }
        case START_TAG: {
          validateNamespace(reader);
          final String element = reader.getName();
          if (!encountered.add(element))
            throw unexpectedContent(reader);
          switch (element) {
            case E_FILTER:
              parseFilterList(reader, filterBuilder);
              break;
            case E_CONDITIONS:
              parseConditions(reader, conditionBuilder);
              break;
            default:
              throw unexpectedContent(reader);
          }
          break;
        }
        default: {
          throw unexpectedContent(reader);
        }
      }
    }
  }

  private static Version parseResourceRoot(final GeodeModuleXmlParser.ResourceRootFactory factory,
      final String rootPath, final XmlPullParser reader, final ModuleSpec.Builder specBuilder)
      throws XmlPullParserException, IOException {
    String name = null;
    String path = null;
    final Set<String> required = new HashSet<>(LIST_A_PATH);
    final int count = reader.getAttributeCount();
    final boolean is1_8 = atLeast1_8(reader);
    for (int i = 0; i < count; i++) {
      validateAttributeNamespace(reader, i);
      final String attribute = reader.getAttributeName(i);
      required.remove(attribute);
      switch (attribute) {
        case A_NAME:
          if (!is1_8)
            name = reader.getAttributeValue(i);
          else
            throw unknownAttribute(reader, i);
          break;
        case A_PATH:
          path = processPath(reader.getAttributeValue(i));
          break;
        default:
          throw unknownAttribute(reader, i);
      }
    }
    if (!required.isEmpty()) {
      throw missingAttributes(reader, required);
    }
    if (name == null)
      name = is1_8 ? "unnamed" : path;

    final MultiplePathFilterBuilder filterBuilder = PathFilters.multiplePathFilterBuilder(true);
    final SystemPropertyConditionBuilder conditionBuilder = new SystemPropertyConditionBuilder();

    final ResourceLoader resourceLoader;

    final Set<String> encountered = new HashSet<>();
    int eventType;
    for (;;) {
      eventType = reader.nextTag();
      switch (eventType) {
        case END_TAG: {
          if (conditionBuilder.resolve()) {
            try {
              resourceLoader = factory.createResourceLoader(rootPath, path, name);
            } catch (IOException e) {
              throw new XmlPullParserException(
                  String.format("Failed to add resource root '%s' at path '%s'", name, path),
                  reader, e);
            }
            specBuilder.addResourceRoot(ResourceLoaderSpec.createResourceLoaderSpec(resourceLoader,
                filterBuilder.create()));
            if (specBuilder.getVersion() == null) {
              return VersionDetection.detectVersion(resourceLoader);
            } else {
              return null;
            }
          } else {
            return null;
          }
        }
        case START_TAG: {
          validateNamespace(reader);
          final String element = reader.getName();
          if (!encountered.add(element))
            throw unexpectedContent(reader);
          switch (element) {
            case E_FILTER:
              parseFilterList(reader, filterBuilder);
              break;
            case E_CONDITIONS:
              parseConditions(reader, conditionBuilder);
              break;
            default:
              throw unexpectedContent(reader);
          }
          break;
        }
        default: {
          throw unexpectedContent(reader);
        }
      }
    }
  }

  /**
   * Parse the attributeValue for a tokenized system property denoted by "${}", resolve the value
   * and replace the
   * attribute with the resolved value
   */
  private static String processPath(String attributeValue) {
    if (attributeValue != null && !attributeValue.isEmpty()) {
      if (attributeValue.startsWith("${")) {
        int endBracketIndex = attributeValue.indexOf("}");
        String systemProperty = attributeValue.substring(2, endBracketIndex);
        String systemPropertyValue = System.getProperty(systemProperty);
        if (attributeValue.length() > endBracketIndex + 1) {
          return systemPropertyValue + attributeValue.substring(endBracketIndex + 1);
        } else {
          return systemPropertyValue;
        }
      }
    }
    return attributeValue;
  }

  private static void parseConditions(XmlPullParser reader,
      SystemPropertyConditionBuilder conditionBuilder) throws XmlPullParserException, IOException {
    if (!atLeast1_7(reader)) {
      throw unexpectedContent(reader);
    }
    assertNoAttributes(reader);
    // xsd:choice
    int eventType;
    for (;;) {
      eventType = reader.nextTag();
      switch (eventType) {
        case END_TAG: {
          return;
        }
        case START_TAG: {
          validateNamespace(reader);
          switch (reader.getName()) {
            case E_PROPERTY_EQUAL:
              parseConditionalProperty(reader, true, conditionBuilder);
              break;
            case E_PROPERTY_NOT_EQUAL:
              parseConditionalProperty(reader, false, conditionBuilder);
              break;
            default:
              throw unexpectedContent(reader);
          }
          break;
        }
        default: {
          throw unexpectedContent(reader);
        }
      }
    }
  }

  private static void parseConditionalProperty(XmlPullParser reader, boolean equal,
      SystemPropertyConditionBuilder builder) throws XmlPullParserException, IOException {
    String name = null;
    String value = null;
    final Set<String> required = new HashSet<>(Arrays.asList(A_NAME, A_VALUE));
    final int count = reader.getAttributeCount();
    for (int i = 0; i < count; i++) {
      validateAttributeNamespace(reader, i);
      final String attribute = reader.getAttributeName(i);
      required.remove(attribute);
      switch (attribute) {
        case A_NAME:
          name = reader.getAttributeValue(i);
          break;
        case A_VALUE:
          value = reader.getAttributeValue(i);
          break;
        default:
          throw unknownAttribute(reader, i);
      }
    }
    if (!required.isEmpty()) {
      throw missingAttributes(reader, required);
    }
    builder.add(name, value, equal);
    // consume remainder of element
    parseNoContent(reader);
  }

  private static void parseFilterList(final XmlPullParser reader,
      final MultiplePathFilterBuilder builder) throws XmlPullParserException, IOException {
    assertNoAttributes(reader);
    // xsd:choice
    int eventType;
    for (;;) {
      eventType = reader.nextTag();
      switch (eventType) {
        case END_TAG: {
          return;
        }
        case START_TAG: {
          validateNamespace(reader);
          switch (reader.getName()) {
            case E_INCLUDE:
              parsePath(reader, true, builder);
              break;
            case E_EXCLUDE:
              parsePath(reader, false, builder);
              break;
            case E_INCLUDE_SET:
              parseSet(reader, true, builder);
              break;
            case E_EXCLUDE_SET:
              parseSet(reader, false, builder);
              break;
            default:
              throw unexpectedContent(reader);
          }
          break;
        }
        default: {
          throw unexpectedContent(reader);
        }
      }
    }
  }

  private static void parsePath(final XmlPullParser reader, final boolean include,
      final MultiplePathFilterBuilder builder) throws XmlPullParserException, IOException {
    String path = null;
    final Set<String> required = new HashSet<>(LIST_A_PATH);
    final int count = reader.getAttributeCount();
    for (int i = 0; i < count; i++) {
      validateAttributeNamespace(reader, i);
      final String attribute = reader.getAttributeName(i);
      required.remove(attribute);
      switch (attribute) {
        case A_PATH:
          path = reader.getAttributeValue(i);
          break;
        default:
          throw unknownAttribute(reader, i);
      }
    }
    if (!required.isEmpty()) {
      throw missingAttributes(reader, required);
    }

    final boolean literal = path.indexOf('*') == -1 && path.indexOf('?') == -1;
    if (literal) {
      if (path.charAt(path.length() - 1) == '/') {
        builder.addFilter(PathFilters.isChildOf(path), include);
      } else {
        builder.addFilter(PathFilters.is(path), include);
      }
    } else {
      builder.addFilter(PathFilters.match(path), include);
    }

    // consume remainder of element
    parseNoContent(reader);
  }

  private static Set<String> parseSet(final XmlPullParser reader)
      throws XmlPullParserException, IOException {
    assertNoAttributes(reader);
    final Set<String> set = new HashSet<>();
    // xsd:choice
    int eventType;
    for (;;) {
      eventType = reader.nextTag();
      switch (eventType) {
        case END_TAG: {
          return set;
        }
        case START_TAG: {
          validateNamespace(reader);
          switch (reader.getName()) {
            case E_PATH:
              parsePathName(reader, set);
              break;
            default:
              throw unexpectedContent(reader);
          }
        }
      }
    }
  }

  private static void parseSet(final XmlPullParser reader, final boolean include,
      final MultiplePathFilterBuilder builder) throws XmlPullParserException, IOException {
    builder.addFilter(PathFilters.in(parseSet(reader)), include);
  }

  private static void parsePathName(final XmlPullParser reader, final Set<String> set)
      throws XmlPullParserException, IOException {
    String name = null;
    final Set<String> required = new HashSet<>(LIST_A_NAME);
    final int count = reader.getAttributeCount();
    for (int i = 0; i < count; i++) {
      validateAttributeNamespace(reader, i);
      final String attribute = reader.getAttributeName(i);
      required.remove(attribute);
      switch (attribute) {
        case A_NAME:
          name = reader.getAttributeValue(i);
          break;
        default:
          throw unknownAttribute(reader, i);
      }
    }
    if (!required.isEmpty()) {
      throw missingAttributes(reader, required);
    }
    set.add(name);

    // consume remainder of element
    parseNoContent(reader);
  }

  private static void parseProperties(final XmlPullParser reader,
      final GeodeModuleXmlParser.PropertiesCallback propsCallback)
      throws XmlPullParserException, IOException {
    assertNoAttributes(reader);
    // xsd:choice
    int eventType;
    for (;;) {
      eventType = reader.nextTag();
      switch (eventType) {
        case END_TAG: {
          return;
        }
        case START_TAG: {
          validateNamespace(reader);
          switch (reader.getName()) {
            case E_PROPERTY: {
              parseProperty(reader, propsCallback);
              break;
            }
            default:
              throw unexpectedContent(reader);
          }
          break;
        }
        default: {
          throw unexpectedContent(reader);
        }
      }
    }
  }

  private static void parseProperty(final XmlPullParser reader,
      final GeodeModuleXmlParser.PropertiesCallback propsCallback)
      throws XmlPullParserException, IOException {
    String name = null;
    String value = null;
    final Set<String> required = new HashSet<>(LIST_A_NAME);
    final int count = reader.getAttributeCount();
    for (int i = 0; i < count; i++) {
      validateAttributeNamespace(reader, i);
      final String attribute = reader.getAttributeName(i);
      required.remove(attribute);
      switch (attribute) {
        case A_NAME:
          name = reader.getAttributeValue(i);
          break;
        case A_VALUE:
          value = reader.getAttributeValue(i);
          break;
        default:
          throw unknownAttribute(reader, i);
      }
    }
    if (!required.isEmpty()) {
      throw missingAttributes(reader, required);
    }
    propsCallback.parsedProperty(name, value == null ? "true" : value);

    // consume remainder of element
    parseNoContent(reader);
  }

  private static void parsePermissions(final XmlPullParser reader, final ModuleLoader moduleLoader,
      final String moduleName, final ModuleSpec.Builder specBuilder)
      throws XmlPullParserException, IOException {
    assertNoAttributes(reader);
    // xsd:choice
    ArrayList<PermissionFactory> list = new ArrayList<>();
    int eventType;
    for (;;) {
      eventType = reader.nextTag();
      switch (eventType) {
        case END_TAG: {
          specBuilder.setPermissionCollection(
              new FactoryPermissionCollection(list.toArray(new PermissionFactory[list.size()])));
          return;
        }
        case START_TAG: {
          validateNamespace(reader);
          switch (reader.getName()) {
            case E_GRANT: {
              parseGrant(reader, moduleLoader, moduleName, list);
              break;
            }
            default:
              throw unexpectedContent(reader);
          }
          break;
        }
        default: {
          throw unexpectedContent(reader);
        }
      }
    }
  }

  private static void parseGrant(final XmlPullParser reader, final ModuleLoader moduleLoader,
      final String moduleName, final ArrayList<PermissionFactory> list)
      throws XmlPullParserException, IOException {
    String permission = null;
    String name = null;
    String actions = null;
    final Set<String> required = new HashSet<>(LIST_A_PERMISSION_A_NAME);
    final int count = reader.getAttributeCount();
    for (int i = 0; i < count; i++) {
      validateAttributeNamespace(reader, i);
      final String attribute = reader.getAttributeName(i);
      required.remove(attribute);
      switch (attribute) {
        case A_PERMISSION:
          permission = reader.getAttributeValue(i);
          break;
        case A_NAME:
          name = reader.getAttributeValue(i);
          break;
        case A_ACTIONS:
          actions = reader.getAttributeValue(i);
          break;
        default:
          throw unknownAttribute(reader, i);
      }
    }
    if (!required.isEmpty()) {
      throw missingAttributes(reader, required);
    }
    expandName(moduleLoader, moduleName, list, permission, name, actions);

    // consume remainder of element
    parseNoContent(reader);
  }

  private static void parseProvidesType(final XmlPullParser reader,
      final ModuleSpec.Builder specBuilder) throws XmlPullParserException, IOException {
    assertNoAttributes(reader);
    int eventType;
    for (;;) {
      eventType = reader.nextTag();
      switch (eventType) {
        case END_TAG: {
          return;
        }
        case START_TAG: {
          validateNamespace(reader);
          switch (reader.getName()) {
            case E_SERVICE: {
              parseProvidedServiceType(reader, specBuilder);
              break;
            }
            default:
              throw unexpectedContent(reader);
          }
          break;
        }
        default: {
          throw unexpectedContent(reader);
        }
      }
    }
  }

  private static void parseProvidedServiceType(final XmlPullParser reader,
      final ModuleSpec.Builder specBuilder) throws XmlPullParserException, IOException {
    String name = null;
    final int count = reader.getAttributeCount();
    for (int i = 0; i < count; i++) {
      validateAttributeNamespace(reader, i);
      final String attribute = reader.getAttributeName(i);
      switch (attribute) {
        case A_NAME:
          name = reader.getAttributeValue(i);
          break;
        default:
          throw unknownAttribute(reader, i);
      }
    }
    if (name == null) {
      throw missingAttributes(reader, Collections.singleton("name"));
    }
    int eventType;
    for (;;) {
      eventType = reader.nextTag();
      switch (eventType) {
        case END_TAG: {
          return;
        }
        case START_TAG: {
          validateNamespace(reader);
          switch (reader.getName()) {
            case E_WITH_CLASS: {
              specBuilder.addProvide(name, parseClassNameType(reader));
              break;
            }
            default:
              throw unexpectedContent(reader);
          }
          break;
        }
        default: {
          throw unexpectedContent(reader);
        }
      }
    }
  }

  private static void expandName(final ModuleLoader moduleLoader, final String moduleName,
      final ArrayList<PermissionFactory> list, String permission, String name, String actions) {
    String expandedName = PolicyExpander.expand(name);
    // If a property can't be expanded in a permission entry that entry is ignored.
    // https://docs.oracle.com/javase/8/docs/technotes/guides/security/PolicyFiles.html#PropertyExp
    if (expandedName != null)
      list.add(new ModularPermissionFactory(moduleLoader, moduleName, permission, expandedName,
          actions));
  }

  private static void parseNoContent(final XmlPullParser reader)
      throws XmlPullParserException, IOException {
    int eventType;
    for (;;) {
      eventType = reader.nextTag();
      switch (eventType) {
        case END_TAG: {
          return;
        }
        default: {
          throw unexpectedContent(reader);
        }
      }
    }
  }

  private static void parseEndDocument(final XmlPullParser reader)
      throws XmlPullParserException, IOException {
    int eventType;
    for (;;) {
      eventType = reader.nextToken();
      switch (eventType) {
        case END_DOCUMENT: {
          return;
        }
        case TEXT:
        case CDSECT: {
          if (!reader.isWhitespace()) {
            throw unexpectedContent(reader);
          }
          // ignore
          break;
        }
        case IGNORABLE_WHITESPACE:
        case COMMENT: {
          // ignore
          break;
        }
        default: {
          throw unexpectedContent(reader);
        }
      }
    }
  }

  private static void unzip(File src, File destDir) throws IOException {
    final String absolutePath = destDir.getAbsolutePath();
    final ZipFile zip = new ZipFile(src);

    try {
      final Enumeration<? extends ZipEntry> entries = zip.entries();

      while (entries.hasMoreElements()) {
        final ZipEntry entry = entries.nextElement();
        if (entry.isDirectory()) {
          continue;
        }

        final File fp =
            new File(absolutePath, PathUtils.canonicalize(PathUtils.relativize(entry.getName())));
        final File parent = fp.getParentFile();
        if (!parent.exists()) {
          parent.mkdirs();
        }
        final InputStream is = zip.getInputStream(entry);
        try {
          final FileOutputStream os = new FileOutputStream(fp);
          try {
            copy(is, os);
          } finally {
            safeClose(os);
          }
        } finally {
          safeClose(is);
        }
      }
    } finally {
      safeClose(zip);
    }
  }

  private static void copy(InputStream in, OutputStream out) throws IOException {
    byte[] buf = new byte[16384];
    int len;
    while ((len = in.read(buf)) > 0) {
      out.write(buf, 0, len);
    }
    out.flush();
  }

  private static void safeClose(Closeable closeable) {
    if (closeable != null)
      try {
        closeable.close();
      } catch (Throwable ignored) {
      }
  }

  static class DefaultResourceRootFactory implements GeodeModuleXmlParser.ResourceRootFactory {

    private DefaultResourceRootFactory() {}

    static final GeodeModuleXmlParser.DefaultResourceRootFactory INSTANCE =
        new GeodeModuleXmlParser.DefaultResourceRootFactory();

    public ResourceLoader createResourceLoader(final String rootPath, final String loaderPath,
        final String loaderName) throws IOException {
      final File file;
      final File loaderFile;
      final String loaderFileName;
      if (File.separatorChar == '/') {
        loaderFileName = loaderPath;
      } else {
        loaderFileName = loaderPath.replace('/', File.separatorChar);
      }
      loaderFile = new File(loaderFileName);
      if (loaderFile.isAbsolute()) {
        file = loaderFile;
      } else {
        final String rootPathName;
        if (File.separatorChar == '/') {
          rootPathName = rootPath;
        } else {
          rootPathName = rootPath.replace('/', File.separatorChar);
        }
        file = new File(rootPathName, loaderFileName);
      }
      if (file.isDirectory()) {
        return ResourceLoaders.createPathResourceLoader(loaderName, file.toPath());
      } else {
        final JarFile jarFile = org.jboss.modules.xml.JDKSpecific.getJarFile(file, true);
        return ResourceLoaders.createJarResourceLoader(loaderName, jarFile);
      }
    }
  }
}
