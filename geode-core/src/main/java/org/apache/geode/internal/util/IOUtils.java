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
package org.apache.geode.internal.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;

import org.apache.commons.lang3.StringUtils;

/**
 * Reusable Input/Output operation utility methods.
 *
 * @since GemFire 6.6
 */
@SuppressWarnings("unused")
public abstract class IOUtils {

  public static final int BUFFER_SIZE = 4096;

  /**
   * Gets a fully qualified path with the path elements appended to the specified pathname using the
   * File.separator character. If the pathname is unspecified (null, empty or blank) then path
   * elements are considered relative to file system root, beginning with File.separator. If array
   * of path elements are null, then the pathname is returned as is.
   *
   * @param pathname a String value indicating the base pathname.
   * @param pathElements the path elements to append to pathname.
   * @return the path elements appended to the pathname.
   * @see java.io.File#separator
   */
  public static String appendToPath(String pathname, final String... pathElements) {
    if (pathElements != null) {
      pathname = StringUtils.defaultIfBlank(pathname, File.separator);

      for (final String pathElement : pathElements) {
        pathname += (pathname.endsWith(File.separator) ? "" : File.separator);
        pathname += pathElement;
      }
    }

    return pathname;
  }

  /**
   * Invokes the close method on any class instance implementing the Closeable interface, such as
   * InputStreams and OutputStreams. Note, this method silently ignores the possible IOException
   * resulting from the close invocation.
   * <p/>
   *
   * @param obj an Object implementing the Closeable interface who's close method will be invoked.
   */
  public static void close(final Closeable obj) {
    if (obj != null) {
      try {
        obj.close();
      } catch (IOException ignore) {
      }
    }
  }

  /**
   * Creates a path with the given path elements delimited with File.separator.
   * </p>
   *
   * @param pathElements an array of Strings constituting elements of the path.
   * @return a fully constructed pathname containing the elements from the given array as path
   *         elements separated by File.separator.
   * @see java.io.File#separator
   */
  public static String createPath(final String... pathElements) {
    return createPath(pathElements, File.separator);
  }

  /**
   * Creates a path with the given path elements delimited with separator.
   * </p>
   *
   * @param pathElements an array of Strings constituting elements of the path.
   * @param separator a String specifying the separator of the path. If the given String is null,
   *        then separator defaults to File.separator
   * @return a fully constructor pathname containing the elements of the path from the given array
   *         separated by separator.
   * @throws NullPointerException if the pathElements is null.
   * @see java.io.File#separator
   */
  public static String createPath(final String[] pathElements, String separator) {
    separator = separator != null ? separator : File.separator;

    final StringBuilder buffer = new StringBuilder();

    for (String pathElement : pathElements) {
      buffer.append(separator).append(pathElement);
    }

    return buffer.toString();
  }

  /**
   * Convenience method to de-serialize a byte array back into Object form.
   * <p/>
   *
   * @param objBytes an array of bytes constituting the serialized form of the Object.
   * @return a Serializable Object from the array of bytes.
   * @throws ClassNotFoundException if the Class type of the serialized Object cannot be resolved.
   * @throws IOException if an I/O error occurs during the de-serialization process.
   * @see #deserializeObject(byte[], ClassLoader)
   * @see #serializeObject(Object)
   * @see java.io.ByteArrayInputStream
   * @see java.io.ObjectInputStream
   * @see java.io.Serializable
   */
  public static Object deserializeObject(final byte[] objBytes)
      throws IOException, ClassNotFoundException {
    ObjectInputStream objIn = null;

    try {
      objIn = new ObjectInputStream(new ByteArrayInputStream(objBytes));
      return objIn.readObject();
    } finally {
      close(objIn);
    }
  }

  /**
   * Convenience method to de-serialize a byte array back into an Object who's Class type is
   * resolved by the specific ClassLoader.
   * <p/>
   *
   * @param objBytes an array of bytes constituting the serialized form of the Object.
   * @param loader the ClassLoader used to resolve the Class type of the serialized Object.
   * @return a Serializable Object from the array of bytes.
   * @throws ClassNotFoundException if the Class type of the serialized Object cannot be resolved by
   *         the specified ClassLoader.
   * @throws IOException if an I/O error occurs while de-serializing the Object from the array of
   *         bytes.
   * @see #deserializeObject(byte[])
   * @see #serializeObject(Object)
   * @see IOUtils.ClassLoaderObjectInputStream
   * @see java.io.ByteArrayInputStream
   * @see java.io.Serializable
   * @see java.lang.ClassLoader
   */
  public static Object deserializeObject(final byte[] objBytes, final ClassLoader loader)
      throws IOException, ClassNotFoundException {
    ObjectInputStream objIn = null;

    try {
      ByteArrayInputStream bis = new ByteArrayInputStream(objBytes);
      objIn = new ClassLoaderObjectInputStream(bis, loader);
      return objIn.readObject();
    } finally {
      close(objIn);
    }
  }

  /**
   * Extracts the filename from the pathname of a file system resource (file).
   * <p/>
   *
   * @param pathname a String indicating the path, or location of the file system resource.
   * @return a String value containing only the filename of the file system resource (file).
   */
  public static String getFilename(final String pathname) {
    String filename = pathname;

    if (StringUtils.isNotBlank(filename)) {
      final int index = filename.lastIndexOf(File.separator);
      filename = (index == -1 ? filename : filename.substring(index + 1));
    }

    return filename;
  }

  /**
   * Determines whether the path represented by name exists in the file system of the localhost.
   * <p/>
   *
   * @param pathname a String indicating the name of the path.
   * @return a boolean indicating whether the path represented by name (pathname) actually exists in
   *         the file system of the localhost (system).
   * @see org.apache.geode.internal.lang.StringUtils#isNotBlank(CharSequence)
   * @see java.io.File#exists()
   */
  public static boolean isExistingPathname(final String pathname) {
    return (StringUtils.isNotBlank(pathname) && new File(pathname).exists());
  }

  /**
   * Convenience method to serialize a Serializable Object into a byte array.
   * <p/>
   *
   * @param obj the Serializable Object to serialize into an array of bytes.
   * @return a byte array of the serialized Object.
   * @throws IOException if an I/O error occurs during the serialization process.
   * @see #deserializeObject(byte[])
   * @see java.io.ByteArrayOutputStream
   * @see java.io.ObjectOutputStream
   * @see java.io.Serializable
   */
  public static byte[] serializeObject(final Object obj) throws IOException {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();

    ObjectOutputStream objOut = null;

    try {
      objOut = new ObjectOutputStream(out);
      objOut.writeObject(obj);
      objOut.flush();

      return out.toByteArray();
    } finally {
      close(objOut);
    }
  }

  /**
   * Reads the contents of the specified InputStream into a byte array.
   * <p/>
   *
   * @param in the InputStream to read content from.
   * @return a byte array containing the content of the specified InputStream.
   * @throws IOException if an I/O error occurs while reading the InputStream.
   * @see java.io.ByteArrayOutputStream
   * @see java.io.InputStream
   */
  public static byte[] toByteArray(final InputStream in) throws IOException {
    assert in != null : "The input stream to read bytes from cannot be null!";

    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    final byte[] buffer = new byte[BUFFER_SIZE];
    int bytesRead;

    try {
      while ((bytesRead = in.read(buffer)) != -1) {
        out.write(buffer, 0, bytesRead);
        out.flush();
      }
    } finally {
      IOUtils.close(in);
      IOUtils.close(out);
    }

    return out.toByteArray();
  }

  /**
   * This method attempts to get the canonical form of the specified file otherwise returns it's
   * absolute form.
   * <p/>
   *
   * @param file the java.io.File object who's canonical representation is attempted to be returned.
   * @return the canonical form of the specified File or the absolute form if an IOException occurs
   *         during the File.getCanonicalFile call.
   * @see java.io.File#getCanonicalFile()
   * @see java.io.File#getAbsoluteFile()
   */
  public static File tryGetCanonicalFileElseGetAbsoluteFile(final File file) {
    try {
      return file.getCanonicalFile();
    } catch (IOException e) {
      return file.getAbsoluteFile();
    }
  }

  /**
   * This method attempts to get the canonical path of the specified file otherwise returns it's
   * absolute path.
   * <p/>
   *
   * @param file the java.io.File object who's canonical path is attempted to be returned.
   * @return the canonical path of the specified File or the absolute path if an IOException occurs
   *         during the File.getCanonicalPath call.
   * @see java.io.File#getCanonicalPath()
   * @see java.io.File#getAbsolutePath()
   */
  public static String tryGetCanonicalPathElseGetAbsolutePath(final File file) {
    try {
      return file.getCanonicalPath();
    } catch (IOException e) {
      return file.getAbsolutePath();
    }
  }

  /**
   * Verifies that the specified pathname is valid and actually exists in the file system in
   * localhost. The pathname is considered valid if it is not null, empty or blank and exists in the
   * file system as a file path (which could represent a file or a directory).
   * </p>
   *
   * @param pathname a String indicating the file path in the file system on localhost.
   * @return the pathname if valid and it exits.
   * @throws FileNotFoundException if the pathname is invalid or does not exist in the file system
   *         on localhost.
   * @see java.io.File#exists()
   */
  public static String verifyPathnameExists(final String pathname) throws FileNotFoundException {
    if (isExistingPathname(pathname)) {
      return pathname;
    }

    throw new FileNotFoundException(String.format("Pathname (%1$s) could not be found!", pathname));
  }

  /**
   * The ClassLoaderObjectInputStream class is a ObjectInputStream implementation that resolves the
   * Class type of the Object being de-serialized with the specified ClassLoader.
   * <p/>
   *
   * @see java.io.ObjectInputStream
   * @see java.lang.ClassLoader
   */
  protected static class ClassLoaderObjectInputStream extends ObjectInputStream {

    private final ClassLoader loader;

    public ClassLoaderObjectInputStream(final InputStream in, final ClassLoader loader)
        throws IOException {
      super(in);

      if (loader == null) {
        throw new NullPointerException(
            "The ClassLoader used by this ObjectInputStream to resolve Class types for serialized Objects cannot be null!");
      }

      this.loader = loader;
    }

    protected ClassLoader getClassLoader() {
      return loader;
    }

    @Override
    protected Class<?> resolveClass(final ObjectStreamClass descriptor)
        throws IOException, ClassNotFoundException {
      return Class.forName(descriptor.getName(), false, getClassLoader());
    }
  }

}
