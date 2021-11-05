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
package org.jboss.modules;

import static java.security.AccessController.doPrivileged;
import static org.jboss.modules.Utils.MODULE_FILE;

import java.io.File;
import java.io.FilePermission;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.UndeclaredThrowableException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Predicate;

import org.jboss.modules.filter.PathFilter;
import org.jboss.modules.filter.PathFilters;
import org.jboss.modules.xml.GeodeModuleXmlParser;

/**
 * This is a shameless copy of {@link LocalModuleFinder}, since it is marked as final and we need to
 * swap out the
 * {@link GeodeModuleXmlParser} with a custom xml parser that can handle Java system properties.
 */

public class GeodeLocalModuleFinderExt implements IterableModuleFinder, AutoCloseable {

  private static final File[] NO_FILES = new File[0];

  private final File[] repoRoots;
  private final PathFilter pathFilter;
  private final AccessControlContext accessControlContext;
  private final List<ResourceLoader> resourceLoaderList = new ArrayList<>(64);
  private final GeodeModuleXmlParser.ResourceRootFactory resourceRootFactory;

  private static final ResourceLoader TERMINATED_MARKER = new ResourceLoader() {
    public String getRootName() {
      return null;
    }

    public ClassSpec getClassSpec(final String fileName) throws IOException {
      return null;
    }

    public PackageSpec getPackageSpec(final String name) throws IOException {
      return null;
    }

    public Resource getResource(final String name) {
      return null;
    }

    public String getLibrary(final String name) {
      return null;
    }

    public Collection<String> getPaths() {
      return null;
    }
  };

  private GeodeLocalModuleFinderExt(final File[] repoRoots, final PathFilter pathFilter,
      final boolean cloneRoots) {
    this.repoRoots = cloneRoots && repoRoots.length > 0 ? repoRoots.clone() : repoRoots;
    final SecurityManager sm = System.getSecurityManager();
    if (sm != null) {
      for (File repoRoot : this.repoRoots) {
        if (repoRoot != null)
          sm.checkPermission(new FilePermission(new File(repoRoot, "-").getPath(), "read"));
      }
    }
    this.pathFilter = pathFilter;
    this.accessControlContext = AccessController.getContext();
    resourceRootFactory = (rootPath, loaderPath, loaderName) -> {
      final ResourceLoader loader = GeodeModuleXmlParser.ResourceRootFactory.getDefault()
          .createResourceLoader(rootPath, loaderPath, loaderName);
      final List<ResourceLoader> list = this.resourceLoaderList;
      synchronized (list) {
        if (list.size() == 1 && list.get(0) == TERMINATED_MARKER) {
          safeClose(loader);
          throw new IllegalStateException("Module finder is closed");
        }
        list.add(loader);
      }
      return loader;
    };
  }

  /**
   * Construct a new instance.
   *
   * @param repoRoots the repository roots to use
   * @param pathFilter the path filter to use
   */
  public GeodeLocalModuleFinderExt(final File[] repoRoots, final PathFilter pathFilter) {
    this(repoRoots, pathFilter, true);
  }

  /**
   * Construct a new instance.
   *
   * @param repoRoots the repository roots to use
   */
  public GeodeLocalModuleFinderExt(final File[] repoRoots) {
    this(repoRoots, PathFilters.acceptAll());
  }

  /**
   * Construct a new instance, using the {@code module.path} system property or the
   * {@code JAVA_MODULEPATH} environment variable
   * to get the list of module repository roots.
   * <p>
   * This is equivalent to a call to {@link LocalModuleFinder#LocalModuleFinder(boolean)
   * LocalModuleFinder(true)}.
   * </p>
   */
  public GeodeLocalModuleFinderExt() {
    this(true);
  }

  /**
   * Construct a new instance, using the {@code module.path} system property or the
   * {@code JAVA_MODULEPATH} environment variable
   * to get the list of module repository roots.
   *
   * @param supportLayersAndAddOns {@code true} if the identified module repository roots should be
   *        checked for
   *        an internal structure of child "layer" and "add-on" directories that may also
   *        be treated as module roots lower in precedence than the parent root. Any "layers"
   *        subdirectories whose names are specified in a {@code layers.conf} file found in
   *        the module repository root will be added in the precedence of order specified
   *        in the {@code layers.conf} file; all "add-on" subdirectories will be added at
   *        a lower precedence than all "layers" and with no guaranteed precedence order
   *        between them. If {@code false} no check for "layer" and "add-on" directories
   *        will be performed.
   *
   */
  public GeodeLocalModuleFinderExt(boolean supportLayersAndAddOns) {
    this(getRepoRoots(supportLayersAndAddOns), PathFilters.acceptAll(), false);
  }

  static File[] getRepoRoots(final boolean supportLayersAndAddOns) {
    return supportLayersAndAddOns
        ? LayeredModulePathFactory.resolveLayeredModulePath(getModulePathFiles())
        : getModulePathFiles();
  }

  private static File[] getModulePathFiles() {
    return getFiles(System.getProperty("module.path", System.getenv("JAVA_MODULEPATH")), 0, 0);
  }

  private static File[] getFiles(final String modulePath, final int stringIdx, final int arrayIdx) {
    if (modulePath == null)
      return NO_FILES;
    final int i = modulePath.indexOf(File.pathSeparatorChar, stringIdx);
    final File[] files;
    if (i == -1) {
      files = new File[arrayIdx + 1];
      files[arrayIdx] = new File(modulePath.substring(stringIdx)).getAbsoluteFile();
    } else {
      files = getFiles(modulePath, i + 1, arrayIdx + 1);
      files[arrayIdx] = new File(modulePath.substring(stringIdx, i)).getAbsoluteFile();
    }
    return files;
  }

  public ModuleSpec findModule(final String name, final ModuleLoader delegateLoader)
      throws ModuleLoadException {
    final String child = PathUtils.basicModuleNameToPath(name);
    if (child == null) {
      return null; // not valid, so not found
    }
    final PathFilter pathFilter = this.pathFilter;
    if (pathFilter.accept(child + "/")) {
      try {
        return doPrivileged(
            (PrivilegedExceptionAction<ModuleSpec>) () -> parseModuleXmlFile(resourceRootFactory,
                name, delegateLoader, repoRoots),
            accessControlContext);
      } catch (PrivilegedActionException e) {
        try {
          throw e.getCause();
        } catch (IOException e1) {
          throw new ModuleLoadException(e1);
        } catch (RuntimeException | Error | ModuleLoadException e1) {
          throw e1;
        } catch (Throwable t) {
          throw new UndeclaredThrowableException(t);
        }
      }
    }
    return null;
  }

  /**
   * Parse a {@code module.xml} file and return the corresponding module specification.
   *
   * @param identifier the identifier to load
   * @param delegateLoader the delegate module loader to use for module specifications
   * @param roots the repository root paths to search
   * @return the module specification
   * @throws IOException if reading the module file failed
   * @throws ModuleLoadException if creating the module specification failed (e.g. due to a parse
   *         error)
   * @deprecated Use {@link #parseModuleXmlFile(String, ModuleLoader, File...)} instead.
   */
  @Deprecated
  public static ModuleSpec parseModuleXmlFile(final ModuleIdentifier identifier,
      final ModuleLoader delegateLoader, final File... roots)
      throws IOException, ModuleLoadException {
    return parseModuleXmlFile(identifier.toString(), delegateLoader, roots);
  }

  /**
   * Parse a {@code module.xml} file and return the corresponding module specification.
   *
   * @param name the name of the module to load
   * @param delegateLoader the delegate module loader to use for module specifications
   * @param roots the repository root paths to search
   * @return the module specification
   * @throws IOException if reading the module file failed
   * @throws ModuleLoadException if creating the module specification failed (e.g. due to a parse
   *         error)
   */
  public static ModuleSpec parseModuleXmlFile(final String name, final ModuleLoader delegateLoader,
      final File... roots) throws IOException, ModuleLoadException {
    return parseModuleXmlFile(GeodeModuleXmlParser.ResourceRootFactory.getDefault(), name,
        delegateLoader, roots);
  }

  static ModuleSpec parseModuleXmlFile(final GeodeModuleXmlParser.ResourceRootFactory factory,
      final String name, final ModuleLoader delegateLoader, final File... roots)
      throws IOException, ModuleLoadException {
    final String child = PathUtils.basicModuleNameToPath(name);
    if (child == null) {
      return null; // not valid, so not found
    }
    for (File root : roots) {
      File file = new File(root, child);
      File moduleXml = new File(file, MODULE_FILE);
      if (moduleXml.exists()) {
        final ModuleSpec spec =
            GeodeModuleXmlParser.parseModuleXml(factory, delegateLoader, name, file, moduleXml);
        if (spec == null)
          break;
        return spec;
      }
    }
    return null;
  }

  private static final Path MODULE_FILE_PATH = new File(MODULE_FILE).toPath();

  private static final Predicate<Path> ITER_FILTER = new Predicate<Path>() {
    public boolean test(final Path path) {
      final Path fileName = path.getFileName();
      return fileName != null && fileName.equals(MODULE_FILE_PATH);
    }
  };

  public Iterator<String> iterateModules(final String baseName, final boolean recursive,
      final ModuleLoader delegateLoader) {
    return new Iterator<String>() {
      private final Iterator<File> rootIter = Arrays.asList(repoRoots).iterator();
      private final Set<String> found = new HashSet<>();
      private Path rootPath;
      private Iterator<Path> pathIter;
      private String next;

      public boolean hasNext() {
        while (next == null) {
          while (pathIter == null) {
            if (!rootIter.hasNext()) {
              return false;
            }
            Path path = rootIter.next().toPath();
            rootPath = path;
            if (baseName != null && !baseName.isEmpty()) {
              path = path.resolve(PathUtils.basicModuleNameToPath(baseName));
            }
            try {
              pathIter = Files.walk(path, recursive ? Integer.MAX_VALUE : 1).filter(ITER_FILTER)
                  .iterator();
            } catch (IOException ignored) {
              pathIter = null;
              continue;
            }
          }
          if (pathIter.hasNext()) {
            final Path nextPath = pathIter.next();
            if (nextPath.getParent() == null) {
              continue;
            }
            try (InputStream stream = Files.newInputStream(nextPath)) {
              final ModuleSpec moduleSpec = GeodeModuleXmlParser.parseModuleXml(
                  GeodeModuleXmlParser.ResourceRootFactory.getDefault(),
                  nextPath.getParent().toString(), stream, nextPath.toString(), delegateLoader,
                  (String) null);
              if (moduleSpec != null) {
                this.next = moduleSpec.getName();
                if (found.add(this.next)) {
                  return true;
                }
                this.next = null;
              }
            } catch (IOException | ModuleLoadException e) {
              // ignore
            }
          } else {
            pathIter = null;
          }
        }
        return true;
      }

      public String next() {
        if (!hasNext())
          throw new NoSuchElementException();
        try {
          return next;
        } finally {
          next = null;
        }
      }
    };
  }

  public String toString() {
    final StringBuilder b = new StringBuilder();
    b.append("local module finder @").append(Integer.toHexString(hashCode())).append(" (roots: ");
    final int repoRootsLength = repoRoots.length;
    for (int i = 0; i < repoRootsLength; i++) {
      final File root = repoRoots[i];
      b.append(root);
      if (i != repoRootsLength - 1) {
        b.append(',');
      }
    }
    b.append(')');
    return b.toString();
  }

  /**
   * Close this module loader and release all backing files. Note that subsequent load attempts will
   * fail with an
   * error after this method is called.
   */
  public void close() {
    final List<ResourceLoader> list = this.resourceLoaderList;
    final ArrayList<ResourceLoader> toClose;
    synchronized (list) {
      if (list.size() == 1 && list.get(0) == TERMINATED_MARKER) {
        return;
      }
      toClose = new ArrayList<>(list);
      list.clear();
      list.add(TERMINATED_MARKER);
    }
    for (ResourceLoader resourceLoader : toClose) {
      safeClose(resourceLoader);
    }
  }

  private static void safeClose(AutoCloseable closeable) {
    if (closeable != null)
      try {
        closeable.close();
      } catch (Throwable ignored) {
      }
  }
}
