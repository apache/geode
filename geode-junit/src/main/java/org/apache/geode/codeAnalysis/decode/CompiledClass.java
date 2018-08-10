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
package org.apache.geode.codeAnalysis.decode;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.Serializable;

import org.apache.geode.DataSerializable;
import org.apache.geode.LogWriter;
import org.apache.geode.codeAnalysis.decode.cp.Cp;
import org.apache.geode.codeAnalysis.decode.cp.CpClass;
import org.apache.geode.codeAnalysis.decode.cp.CpDouble;
import org.apache.geode.codeAnalysis.decode.cp.CpLong;
import org.apache.geode.internal.DataSerializableFixedID;
import org.apache.geode.internal.ExitCode;
import org.apache.geode.internal.logging.PureLogWriter;


/**
 * Decoder represents a jdk ClassFile header
 */

public class CompiledClass implements Comparable {
  public long magic;
  public int minor_version;
  public int major_version;
  public int constant_pool_count;
  public Cp constant_pool[];
  public int access_flags;
  public int this_class; // ptr into constant_pool
  public int super_class; // ptr into constant_pool
  public int interfaces_count;
  public int interfaces[];
  public int fields_count;
  public CompiledField fields[];
  public int methods_count;
  public CompiledMethod methods[];
  public int attributes_count;
  public CompiledAttribute attributes[];

  static LogWriter debugLog;
  static PrintStream debugStream;

  static {
    try {
      debugStream = new PrintStream("loadedClasses.log");
      debugLog = new PureLogWriter(PureLogWriter.ALL_LEVEL, debugStream);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
  }

  public static CompiledClass getInstance(File classFile) throws IOException {
    FileInputStream fstream = new FileInputStream(classFile);
    DataInputStream source = new DataInputStream(fstream);
    CompiledClass instance = new CompiledClass(source);
    fstream.close();
    return instance;
  }

  public static CompiledClass getInstance(InputStream classStream) throws IOException {
    DataInputStream source = new DataInputStream(classStream);
    CompiledClass instance = new CompiledClass(source);
    classStream.close();
    return instance;
  }

  /**
   * read a ClassFile structure from the given input source (usually a file)
   */
  public CompiledClass(DataInputStream source) throws IOException {
    int idx;

    magic = (long) source.readInt();
    minor_version = source.readUnsignedShort();
    major_version = source.readUnsignedShort();
    // the first constant-pool slot is reserved by Java and is not in the classes file
    constant_pool_count = source.readUnsignedShort();
    // read the constant pool list
    constant_pool = new Cp[constant_pool_count];
    constant_pool[0] = null;
    for (idx = 1; idx < constant_pool_count; idx++) {
      constant_pool[idx] = Cp.readCp(source);
      // from Venkatesh: workaround for Javasoft hack
      // From the Java Virtual Machine Specification,
      // all eight-byte constants take up two spots in the constant pool.
      // If this is the nth item in the constant pool, then the next
      // item will be numbered n+2.
      if ((constant_pool[idx] instanceof CpLong) || (constant_pool[idx] instanceof CpDouble))
        idx++;
    }
    access_flags = source.readUnsignedShort();
    this_class = source.readUnsignedShort();
    super_class = source.readUnsignedShort();
    interfaces_count = source.readUnsignedShort();
    // read the interfaces list (ptrs into constant_pool)
    interfaces = new int[interfaces_count];
    for (idx = 0; idx < interfaces_count; idx++) {
      interfaces[idx] = source.readUnsignedShort();
    }
    fields_count = source.readUnsignedShort();
    // read the fields list (only includes this class, not inherited variables)
    fields = new CompiledField[fields_count];
    for (idx = 0; idx < fields_count; idx++) {
      fields[idx] = new CompiledField(source, this);
    }
    methods_count = source.readUnsignedShort();
    // read the methods list
    methods = new CompiledMethod[methods_count];
    for (idx = 0; idx < methods_count; idx++) {
      methods[idx] = new CompiledMethod(source, this);
    }
    attributes_count = source.readUnsignedShort();
    // read the attributes
    attributes = new CompiledAttribute[attributes_count];
    for (idx = 0; idx < attributes_count; idx++) {
      attributes[idx] = new CompiledAttribute(source);
    }
  }

  String accessString() {
    StringBuffer result;

    result = new StringBuffer();
    if ((access_flags & 0x0001) != 0)
      result.append("public ");
    if ((access_flags & 0x0002) != 0)
      result.append("(private?) ");
    if ((access_flags & 0x0004) != 0)
      result.append("(protected?) ");
    if ((access_flags & 0x0008) != 0)
      result.append("(static?) ");
    if ((access_flags & 0x0010) != 0)
      result.append("final ");
    if ((access_flags & 0x0020) != 0)
      result.append("(??0x20??) ");
    if ((access_flags & 0x0040) != 0)
      result.append("(volatile?) ");
    if ((access_flags & 0x0080) != 0)
      result.append("(transient?) ");
    if ((access_flags & 0x0100) != 0)
      result = result.append("(??0x100??) ");
    if ((access_flags & 0x0200) != 0)
      result = result.append("interface ");
    if ((access_flags & 0x0400) != 0)
      result = result.append("abstract ");
    if ((access_flags & 0x0800) != 0)
      result = result.append("(??0x800??) ");

    return result.toString();
  }

  public boolean isInterface() {
    return (access_flags & 0x0200) != 0;
  }

  public boolean isSerializableAndNotDataSerializable() {
    // these classes throw exceptions or log ugly messages when you try to load them
    // in junit
    String name = fullyQualifiedName().replace('/', '.');
    if (name.startsWith("org.apache.geode.internal.shared.NativeCallsJNAImpl")
        || name.startsWith("org.apache.geode.internal.statistics.HostStatHelper")) {
      return false;
    }
    try {
      debugLog.info("isSerializableAndNotDataSerializable loading class " + name);
      debugStream.flush();
      Class realClass = Class.forName(name);
      return Serializable.class.isAssignableFrom(realClass)
          && !DataSerializable.class.isAssignableFrom(realClass)
          && !DataSerializableFixedID.class.isAssignableFrom(realClass);
    } catch (UnsatisfiedLinkError e) {
      System.out.println("Unable to load actual class " + name + " external JNI dependencies");
    } catch (NoClassDefFoundError e) {
      System.out.println("Unable to load actual class " + name + " not in JUnit classpath");
    } catch (Throwable e) {
      System.out.println("Unable to load actual class " + name + ": " + e);
    }
    return false;
  }

  public String fullyQualifiedName() {
    return ((CpClass) constant_pool[this_class]).className(this);
  }

  public String superClassName() {
    return ((CpClass) constant_pool[super_class]).className(this);
  }

  public int compareTo(Object other) {
    if (!(other instanceof CompiledClass)) {
      return -1;
    }
    String otherName = ((CompiledClass) other).fullyQualifiedName();
    return this.fullyQualifiedName().compareTo(otherName);
  }

  public static void main(String argv[]) {
    File classFile;
    CompiledClass instance;
    int idx;

    classFile = null;
    try {
      classFile = new File(argv[0]);
    } catch (NullPointerException e) {
      System.err.println("You must give the name of a class file on the command line");
      System.exit(3);
    }
    if (classFile == null) {
      System.err.println("Unable to access " + argv[0]);
      System.exit(3);
    }
    if (!classFile.canRead()) {
      System.err.println("Unable to read " + argv[0]);
      System.exit(3);
    }
    try {
      instance = getInstance(classFile);
      System.out.println("Class name is " + instance.fullyQualifiedName());
      System.out.println("Class access is " + instance.accessString());
      System.out.println("Superclass name is " + instance.superClassName());
      System.out.println("Fields:");
      for (idx = 0; idx < instance.fields_count; idx++) {
        System.out.println("    " + instance.fields[idx].signature());
      }
      System.out.println("Methods:");
      for (idx = 0; idx < instance.methods_count; idx++) {
        System.out.println("    " + instance.methods[idx].signature());
        // if (idx == 0) {
        System.out.println("..method attributes");
        for (int i = 0; i < instance.methods[idx].attributes_count; i++) {
          System.out.println(".." + instance.methods[idx].attributes[i].name(instance));
        }
        // }
      }
    } catch (Throwable e) {
      System.err.println("Error reading file: " + e.getMessage());
      System.exit(3);
    }
    ExitCode.NORMAL.doSystemExit();
  }

}
