package com.gemstone.gemfire.internal.tools.gfsh.app.misc.util;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.internal.tools.gfsh.app.pogo.KeyType;
import com.gemstone.gemfire.internal.tools.gfsh.app.pogo.KeyTypeManager;

/**
 * SystemClassPathManager assigns the class path to the system class loader 
 * in runtime.
 * @author dpark
 *
 */
public class SystemClassPathManager
{
	private static final Class[] parameters = new Class[] { URL.class };

	public static void addFile(String s) throws IOException
	{
		File f = new File(s);
		addFile(f);
	}

	public static void addFile(File f) throws IOException
	{
		addURL(f.toURI().toURL());
	}

	public static void addURL(URL u) throws IOException
	{

		URLClassLoader sysloader = (URLClassLoader) ClassLoader.getSystemClassLoader();
		Class sysclass = URLClassLoader.class;

		try {
			Method method = sysclass.getDeclaredMethod("addURL", parameters);
			method.setAccessible(true);
			method.invoke(sysloader, new Object[] { u });
		      } catch (VirtualMachineError e) {
		        SystemFailure.initiateFailure(e);
		        throw e;
		      } catch (Throwable t) {
		        SystemFailure.checkFailure();
			t.printStackTrace();
			throw new IOException("Error, could not add URL to system classloader");
		}

	}
	
	/**
	 * Includes all of the jar files in the specified directory in the
	 * class path. It first includes the latest dated jar files that end
	 * with the extension '.vyyyyMMddHHmm.jar'. For example, if there are 
	 * 'foo.v201010231217' and 'foo.v201010221011' then only the formal is 
	 * added in the class path since it has the latest version date.
	 * <p>
	 * Once all of the date-versioned jar files are added, it then proceed
	 * to add the rest of the jar files in sorted order.
	 * <p>
	 * It also auto registers versioned classes such as MapLite's KeyType.
	 * 
	 * @param dirPath The absolute or relative directory path.
	 */
	public static void addJarsInDir(String dirPath)
	{
		if (dirPath == null) {
			return;
		}
		
		File classDirFile = new File(dirPath);
		classDirFile.mkdirs();
		
		ArrayList<String> jarList = new ArrayList();
		File[] files = classDirFile.listFiles();
		for (File file : files) {
			if (file.isFile()) {
				String fileName = file.getName();
				jarList.add(file.getAbsolutePath());
			}
		}
		
		// Register the latest files only
		Collections.sort(jarList);
		String prevFileNameNoDate = "";
		ArrayList<File> datedFiles = new ArrayList();
		ArrayList<File> notDatedFiles = new ArrayList();
		for (int i = jarList.size() - 1; i >= 0; i--) {
			String filePath = jarList.get(i);
			if (filePath.endsWith(".jar") == false) {
				continue;
			}
			File file = new File(filePath);
			String fileName = file.getName();
			String nameNoExtension = fileName.substring(0, fileName.lastIndexOf(".jar"));
			int index = nameNoExtension.lastIndexOf(".v");
			if (index == -1) {
				// not dated
				notDatedFiles.add(file);
				continue;
			}
			String fileNameNoDate = nameNoExtension.substring(0, index);
			if (fileNameNoDate.equals(prevFileNameNoDate) == false) {
				try {
					SystemClassPathManager.addFile(file);
					datedFiles.add(file);
				} catch (IOException e) {
					CacheFactory.getAnyInstance().getLogger().error(e);
				}
				prevFileNameNoDate = fileNameNoDate;
			}
		}
		
		// Add the not dated files - dated files take precedence
		Collections.sort(notDatedFiles);
		for (File file : notDatedFiles) {
			try {
				SystemClassPathManager.addFile(file);
			} catch (IOException e) {
				CacheFactory.getAnyInstance().getLogger().error(e);
			}
		}
		
		// Register KeyTypes for dated classes
		registerKeyType(datedFiles);
		
		// Register KeyTypes for not dated classes
		registerKeyType(notDatedFiles);
		
		// for gc
		datedFiles.clear();
		datedFiles = null;
		notDatedFiles.clear();
		notDatedFiles = null;
		jarList.clear();
		jarList = null;
	}
	
	private static void registerKeyType(List<File> files)
	{
		for (File file : files) {
			try {
				Class[] classes = ClassFinder.getAllClasses(file.getAbsolutePath());
				for (int j = 0; j < classes.length; j++) {
					Class<?> cls = classes[j];
					if (KeyType.class.isAssignableFrom(cls) && 
						cls.getSimpleName().matches(".*_v\\d++$")) 
					{
						try {
							Method method = cls.getMethod("getKeyType", (Class[])null);
							KeyType keyType = (KeyType)method.invoke(cls, (Object[])null);
							KeyTypeManager.registerSingleKeyType(keyType);
						} catch (Exception ex) {
							// ignore
						}
					}
				}
			} catch (Exception ex) {
				// ignore
			}
		}	
	}
}
