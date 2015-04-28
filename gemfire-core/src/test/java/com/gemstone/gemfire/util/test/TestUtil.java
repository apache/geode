package com.gemstone.gemfire.util.test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

import junit.framework.AssertionFailedError;

import com.gemstone.gemfire.internal.FileUtil;

public class TestUtil {
  
  /**
   * Return the path to a named resource. This finds the resource on the classpath
   * using the rules of class.getResource. For a relative path it will look in the
   * same package as the class, for an absolute path it will start from the base
   * package.
   * 
   * Best practice is to put the resource in the same package as your test class
   * and load it with this method.
   *  
   * @param clazz the class to look relative too
   * @param name the name of the resource, eg "cache.xml"
   */
  public static String getResourcePath(Class<?> clazz, String name) {
    URL resource = clazz.getResource(name);
    if(resource == null) {
      throw new AssertionFailedError("Could not find resource " + name);
    }
    try {
      String path = resource.toURI().getPath();
      if(path == null) {
        String filename = name.replaceFirst(".*/", "");
        File tmpFile = File.createTempFile(filename, null);
        tmpFile.deleteOnExit();
        FileUtil.copy(resource, tmpFile);
        return tmpFile.getAbsolutePath();
      }
      return path;
    } catch (URISyntaxException | IOException e) {
      throw new RuntimeException("Failed getting path to resource " + name, e);
    }
  }

  private TestUtil() {
    
  }
}
