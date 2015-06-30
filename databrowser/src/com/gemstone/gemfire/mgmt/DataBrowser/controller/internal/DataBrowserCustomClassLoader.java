package com.gemstone.gemfire.mgmt.DataBrowser.controller.internal;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.mgmt.DataBrowser.utils.LogUtil;

/**
 * 
 * @author mjha
 *
 */
public class DataBrowserCustomClassLoader extends URLClassLoader {

  public DataBrowserCustomClassLoader(URL[] urls, ClassLoader parent) {
    super(urls, parent);
  }
  
  public void addURL(URL url) {
    super.addURL(url);
  }
  
  public static String getAuthProp(String jar, Class<?> authInitialize) throws ClassNotFoundException, IOException {
    String authInitializeProperty = null;
    Class<?> authInitializeImplementation = null;
    File file = new File(jar);
    //TODO need to fix this before release
    try {
      URLClassLoader classloader =getUrlClassLoader(file);
      JarFile jarFile = new JarFile(file);
      Enumeration<JarEntry> entries = jarFile.entries();
      while (entries.hasMoreElements()) {
        JarEntry nextElement = entries.nextElement();
        if (!nextElement.isDirectory()) {
          if (nextElement.getName().contains(".class")) {
            Class<?> loadClass = classloader.loadClass(nextElement.getName()
                .replace("/", ".").replace(".class", ""));
            if (authInitializeImplementation== null) {
              Class<?>[] interfaces = loadClass.getInterfaces();
              for (int i = 0; i < interfaces.length; i++) {
                if (interfaces[i].equals(authInitialize)) {
                  authInitializeImplementation = loadClass;
                  break;
                }
              }
            }
          }
        }
        if(authInitializeImplementation != null)
          break;
      }
    }
    catch (IOException e) {
      throw e;
    }
    catch (ClassNotFoundException e) {
      throw e;
    }
    
    if(authInitializeImplementation != null){
      Method[] methods = authInitializeImplementation.getMethods();
      for (int i = 0; i < methods.length; i++) {
        boolean staticMethod = isStaticMethod(methods[i]);
        if(staticMethod){
          if(methods[i].getReturnType().isAssignableFrom(authInitialize)){
            authInitializeProperty = authInitializeImplementation.getName()+ "." + methods[i].getName();

            break;
          }
        }
      }
    }else{
      throw new ClassNotFoundException("Class Implementing 'AuthInitialize' not found in specified security jar");
    }
    return authInitializeProperty;
  }
  
  public static boolean isStaticMethod(Method method){
    return Modifier.isStatic(method.getModifiers());
  }
  
  
  /**
   * 
   * This API is provided to load application class once, so that is application
   * class is registering themselves with Instantiators, will register
   * themselves to instantiator here. This is suppose to be call before query
   * execution, if appluation class is changed
   * 
   * @param jars
   * @throws ClassNotFoundException
   * @throws IOException
   */
  public static void loadApplicationClass( DataBrowserCustomClassLoader classloader, String[] jars)
      throws ClassNotFoundException, IOException {
    for (int i = 0; i < jars.length; i++) {
      String jar = jars[i];
      File file = new File(jar);

      try {
        JarFile jarFile = new JarFile(file);
        Enumeration<JarEntry> entries = jarFile.entries();
        while (entries.hasMoreElements()) {
          JarEntry nextElement = entries.nextElement();
          if (!nextElement.isDirectory()) {
            if (nextElement.getName().contains(".class")) {
              String className = nextElement.getName().replace("/", ".")
              .replace(".class", "");
              try {
                Class<?> forName = Class.forName(className, true, classloader);
                boolean canBeAssignedToDataSerializer = canBeAssignedToDataSerializable(forName);
                if(canBeAssignedToDataSerializer){
                  LogUtil.fine("class " + className + " is loaded");
                }
              }
              catch (ClassNotFoundException e) {
                LogUtil.fine("Error while loading the classes :", e);
              }
              catch (Throwable e) {
                LogUtil.fine("Error while loading the classes :", e);
              }
            }
          }
        }
      }
      catch (IOException e) {
        LogUtil.warning("Error while loading application class " +e);
      }
      catch (Throwable e) {
       LogUtil.warning("Error while loading application class " +e);
      }
    }
  }
  
  private static URLClassLoader getUrlClassLoader(File file) throws IOException{
    //TODO need to fix this before release
    URLClassLoader classloader = new URLClassLoader(new URL[] { file
        .toURI().toURL() }, ClassLoader.getSystemClassLoader());
    
    return classloader;
  }
  
  
  private static boolean canBeAssignedToDataSerializable(Class<?> cls){
    return DataSerializable.class.isAssignableFrom(cls);
  }
  
}
