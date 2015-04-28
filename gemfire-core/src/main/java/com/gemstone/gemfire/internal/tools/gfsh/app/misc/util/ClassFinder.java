package com.gemstone.gemfire.internal.tools.gfsh.app.misc.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

import com.gemstone.gemfire.internal.ClassPathLoader;

public class ClassFinder
{

	/**
	 * Returns all classes that are in the specified package. Use this method to find
	 * inflated classes, i.e., classes that are kept in a directory. Use
	 * getClasses(String jarPath, String packageName) if the classes are in a jar file.
	 * 
	 * @param packageName
	 *            The base package
	 * @return The classes
	 * @throws ClassNotFoundException Thrown if unable to load a class
	 * @throws IOException Thrown if error occurs while reading the jar file
	 */
	public static Class[] getClasses(String packageName) throws ClassNotFoundException, IOException
	{
		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
//		assert classLoader != null;
		String path = packageName.replace('.', '/');
		Enumeration resources = classLoader.getResources(path);
		List dirs = new ArrayList();
		while (resources.hasMoreElements()) {
			URL resource = (URL)resources.nextElement();
			dirs.add(new File(resource.getFile()));
		}
		ArrayList classes = new ArrayList();
		for (Iterator iterator = dirs.iterator(); iterator.hasNext();) {
			File directory = (File)iterator.next();
			classes.addAll(findClasses(directory, packageName));
		}
		return (Class[])classes.toArray(new Class[classes.size()]);
	}

	/**
	 * Returns all classes found in the specified directory.
	 * 
	 * @param directory
	 *            The base directory
	 * @param packageName
	 *            The package name for classes found inside the base directory
	 * @return The classes 
	 * @throws ClassNotFoundException Thrown if unable to load a class
	 */
	public static List findClasses(File directory, String packageName) throws ClassNotFoundException
	{
		List classes = new ArrayList();
		if (!directory.exists()) {
			return classes;
		}
		File[] files = directory.listFiles();
		for (int i = 0; i < files.length; i++) {
			File file = files[i];
			if (file.isDirectory()) {
//				assert !file.getName().contains(".");
				classes.addAll(findClasses(file, packageName + "." + file.getName()));
			} else if (file.getName().endsWith(".class")) {
				classes
						.add(Class
								.forName(packageName + '.' + file.getName().substring(0, file.getName().length() - 6)));
			}
		}
		return classes;
	}

	/**
	 * Returns all classes that are in the specified jar and package name.
	 * @param jarPath The absolute or relative jar path.
	 * @param packageName The package name.
	 * @return Returns all classes that are in the specified jar and package name.
	 * @throws ClassNotFoundException Thrown if unable to load a class
	 * @throws IOException Thrown if error occurs while reading the jar file
	 */
	public static Class[] getClasses(String jarPath, String packageName) throws ClassNotFoundException, IOException
	{
		String[] classNames = getClassNames(jarPath, packageName);
		Class classes[] = new Class[classNames.length];
		for (int i = 0; i < classNames.length; i++) {
			String className = (String)classNames[i];
			classes[i] = Class.forName(className);
		}
		return classes;
	}
	
	/**
	 * Returns all names of classes that are defined in the specified jar and package name.
	 * @param jarPath The absolute or relative jar path.
	 * @param packageName The package name.
	 * @return Returns all names of classes that are defined in the specified jar and package name.
	 * @throws IOException Thrown if error occurs while reading the jar file
	 */
	public static String[] getClassNames(String jarPath, String packageName) throws IOException
	{
		if (jarPath == null) {
			return new String[0];
		}
		
		File file;
		if (jarPath.startsWith("/") || jarPath.indexOf(':') >= 0) {
			// absolute path
			file = new File(jarPath);
		} else {
			// relative path
			String workingDir = System.getProperty("user.dir");
			file = new File(workingDir + "/" + jarPath);
		}
		
		ArrayList arrayList = new ArrayList();
		packageName = packageName.replaceAll("\\.", "/");
		JarInputStream jarFile = new JarInputStream(new FileInputStream(file));
		JarEntry jarEntry;
		while (true) {
			jarEntry = jarFile.getNextJarEntry();
			if (jarEntry == null) {
				break;
			}
			String name = jarEntry.getName();
			if (name.startsWith(packageName) && (name.endsWith(".class"))) {
				int endIndex = name.length() - 6;
				name = name.replaceAll("/", "\\.");
				name = name.substring(0, endIndex);
				arrayList.add(name);
			}
		}
		jarFile.close();

		return (String[])arrayList.toArray(new String[0]);
	}
	
	/**
	 * Returns all classes that are in the specified jar.
	 * @param jarPath The absolute or relative jar path.
	 * @return Returns all classes that are in the specified jar
	 * @throws ClassNotFoundException Thrown if unable to load a class
	 * @throws IOException Thrown if error occurs while reading the jar file
	 */
	public static Class[] getAllClasses(String jarPath) throws ClassNotFoundException, IOException
	{
		String[] classNames = getAllClassNames(jarPath);
		Class classes[] = new Class[classNames.length];
		for (int i = 0; i < classNames.length; i++) {
			String className = (String)classNames[i];
			classes[i] = ClassPathLoader.getLatest().forName(className);
		}
		return classes;
	}
	
	/**
	 * Returns all names of classes that are defined in the specified jar.
	 * @param jarPath The absolute or relative jar path.
	 * @return Returns all names of classes that are defined in the specified jar.
	 * @throws IOException Thrown if error occurs while reading the jar file
	 */
	public static String[] getAllClassNames(String jarPath) throws IOException
	{
		if (jarPath == null) {
			return new String[0];
		}
		
		jarPath = jarPath.trim();
		if (jarPath.length() == 0) {
			return new String[0];
		}
		
		File file;
		if (jarPath.startsWith("/") || jarPath.indexOf(':') >= 0) {
			// absolute path
			file = new File(jarPath);
		} else {
			// relative path
			String workingDir = System.getProperty("user.dir");
			file = new File(workingDir + "/" + jarPath);
		}
		
		ArrayList arrayList = new ArrayList();
		JarInputStream jarFile = new JarInputStream(new FileInputStream(file));
		JarEntry jarEntry;
		while (true) {
			jarEntry = jarFile.getNextJarEntry();
			if (jarEntry == null) {
				break;
			}
			String name = jarEntry.getName();
			if (name.endsWith(".class")) {
				int endIndex = name.length() - 6;
				name = name.replaceAll("/", "\\.");
				name = name.substring(0, endIndex);
				arrayList.add(name);
			}
		}
		jarFile.close();

		return (String[])arrayList.toArray(new String[0]);
	}
}
