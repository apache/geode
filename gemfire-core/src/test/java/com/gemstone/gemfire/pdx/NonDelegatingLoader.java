/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.pdx;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import com.gemstone.gemfire.internal.HeapDataOutputStream;
import com.gemstone.gemfire.internal.Version;

class NonDelegatingLoader extends ClassLoader {

  
  public NonDelegatingLoader(ClassLoader parent) {
    super(parent);
  }

  @Override
  public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
    if(!name.contains("SeparateClassloaderPdx")) {
      return super.loadClass(name, resolve);
    }
    URL url = super.getResource(name.replace('.', File.separatorChar) + ".class");
    if(url == null) {
      throw new ClassNotFoundException();
    }
    HeapDataOutputStream hoas = new HeapDataOutputStream(Version.CURRENT);
    InputStream classStream;
    try {
      classStream = url.openStream();
      while(true) {
        byte[] chunk = new byte[1024];
        int read = classStream.read(chunk);
        if(read < 0) {
          break;
        }
        hoas.write(chunk, 0, read);
      }
    } catch (IOException e) {
      throw new ClassNotFoundException("Error reading class", e);
    }
    
    Class clazz = defineClass(name, hoas.toByteBuffer(), null);
    if(resolve) {
      resolveClass(clazz);
    }
    return clazz;
  }
  
}