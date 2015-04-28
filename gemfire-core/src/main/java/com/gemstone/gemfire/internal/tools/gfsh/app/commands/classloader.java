package com.gemstone.gemfire.internal.tools.gfsh.app.commands;

import java.io.File;
import java.io.FileFilter;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.Instantiator;
import com.gemstone.gemfire.internal.tools.gfsh.app.CommandExecutable;
import com.gemstone.gemfire.internal.tools.gfsh.app.Gfsh;
import com.gemstone.gemfire.internal.tools.gfsh.app.cache.InstantiatorClassLoader;
import com.gemstone.gemfire.internal.tools.gfsh.app.misc.util.ClassFinder;
import com.gemstone.gemfire.internal.tools.gfsh.app.pogo.KeyType;
import com.gemstone.gemfire.internal.tools.gfsh.app.pogo.KeyTypeManager;

public class classloader implements CommandExecutable
{
	private Gfsh gfsh;
	
	public classloader(Gfsh gfsh)
	{
		this.gfsh = gfsh;
	}
	
	public void help()
	{
		gfsh.println("class [-c <fully-qualified class name>] [-id <class id>]");
		gfsh.println("      [-d  <DataSerializables.txt>");
		gfsh.println("      [-jar <jar paths>]");
		gfsh.println("      [-dir <dir path>]");
		gfsh.println("      [-?]");
		gfsh.println("   Load Instantiator registered data classes. All data classes");
		gfsh.println("   that use a static block to register class ids via Instantiator");
		gfsh.println("   must be preloaded using this command. Note that this command");
		gfsh.println("   is NOT equivalent to setting CLASSPATH. As always, the classes");
		gfsh.println("   and jar files must be in the class path before starting gfsh.");
		gfsh.println("   Please run 'gfsh -?' to see more details.");
		gfsh.println("     -c <fully-qualified class name> Load the specified class.");
		gfsh.println("           The specified class typically contains a static block");
		gfsh.println("           that registers class ids using GemFire Instantiator.");
		gfsh.println("     -id <class id> if the class ID for the cass name specified with");
		gfsh.println("            the option '-c' is not defined by Instantiator then");
		gfsh.println("            the '-id' option must be used to assign the class id");
		gfsh.println("            that matches the instantiator class id defined in the");
		gfsh.println("            server's cache.xml file. This options is supported for");
		gfsh.println("            GFE 6.x or greater.");
		gfsh.println("     -d <DataSerializables.txt> Load the classes listed");
		gfsh.println("           in the specified file. The file path can be relative");
		gfsh.println("           or absolute.");
		gfsh.println("     -jar <jar paths> Load all classes in the jar paths. The jar paths");
		gfsh.println("            can be separated by ',', ';', or ':'. The jar paths");
		gfsh.println("            can be relative or absolute.");
		gfsh.println("     -dir <directory> Load all classes in the directory.");
		gfsh.println("            The directory path can be relative or absolute.");
		gfsh.println();
	}
	
	public void execute(String command) throws Exception
	{
		if (command.startsWith("class -?")) {
			help();
		} else if (command.startsWith("class -c")) {
			class_c(command);
		} else if (command.startsWith("class -id")) {
			class_c(command);
		} else if (command.startsWith("class -dir")) {
			class_dir(command);
		} else if (command.startsWith("class -d")) {
			class_d(command);
		} else if (command.startsWith("class -jar")) {
			class_jar(command);
		} else {
			gfsh.println("Error: invalid command - " + command);
		}
	}
	
	private void class_c(String command)
	{
		ArrayList list = new ArrayList();
		gfsh.parseCommand(command, list);
		
		if (list.size() < 2) {
			gfsh.println("Error: must specify an option. Run 'class -?' for options");
			return;
		}
		if (list.size() < 3) {
			if (list.get(1).equals("-c")) {
				gfsh.println("Error: must specify a fully-qualified class name");
			} else if (list.get(1).equals("-id")) {
				gfsh.println("Error: must specify the class id");
			}
			return;
		}
		
		String className = null;
		boolean classIdSpecified = false;
		int classId = 0;
		for (int i = 1; i < list.size(); i++) {
			String val = (String)list.get(i);
			if (val.equals("-c")) {
				i++;
				if (list.size() <= i) {
					gfsh.println("Error: class name not specified");
					return;
				}
				className = (String)list.get(i);
			} else if (val.equals("-id")) {
				i++;
				if (list.size() <= i) {
					gfsh.println("Error: class id not specified");
					return;
				}
				classIdSpecified = true;
				try {
					classId = Integer.parseInt((String)list.get(i));
				} catch (Exception ex) {
					gfsh.println("Error: " + ex.getMessage());
				}
			}
		}
		
		if (className == null) {
			gfsh.println("Error: class name not specified");
			return;
		}
		
		try {
			final Class clazz = Class.forName(className);
			if (classIdSpecified) {
				Instantiator.register(new Instantiator(clazz, classId)
				{
					public DataSerializable newInstance()
					{
						DataSerializable obj = null;
						try {
							obj =  (DataSerializable)clazz.newInstance();
						} catch (Exception ex) {
							gfsh.println("Error: unable to create a new instance of " + clazz.getCanonicalName());
							if (gfsh.isDebug()) {
								ex.printStackTrace();
							}
						}
						return obj;
					}
				});
			}
			gfsh.println("class loaded: " + clazz.getName());
		} catch (Exception e) {
			gfsh.println("Error: " + e.getClass().getSimpleName() + " - " + e.getMessage());
			if (gfsh.isDebug()) {
				e.printStackTrace();
			}
		}
	}
	
	private void class_d(String command)
	{
		ArrayList list = new ArrayList();
		gfsh.parseCommand(command, list);
		if (list.size() < 3) {
			gfsh.println("Error: must specify the file path of DataSerializables.txt");
			return;
		}
		
		String filePath = (String)list.get(2);
		
		try {
			InstantiatorClassLoader.loadDataSerializables(filePath);
			gfsh.println();
			gfsh.println("application classes successfully loaded: " + filePath);
		} catch (Exception e) {
			gfsh.println("Error: " + e.getClass().getSimpleName() + " - " + e.getMessage() + ". Aborted.");
			if (gfsh.isDebug()) {
				e.printStackTrace();
			}
		}
	}

	private void class_jar(String command)
	{
		ArrayList list = new ArrayList();
		gfsh.parseCommand(command, list);
		if (list.size() < 3) {
			gfsh.println("Error: must specify the jar paths");
			return;
		}
		
		String jarPaths = (String)list.get(2);
		if (jarPaths == null) {
			return;
		}
		jarPaths = jarPaths.trim();
		if (jarPaths.length() == 0) {
			return;
		}
		
		String pathSeparator = System.getProperty("path.separator");
		try {
			jarPaths = jarPaths.replace(pathSeparator.charAt(0), ',');
			String split[] = jarPaths.split(",");
			URL url[] = new URL[split.length];
			ArrayList<String> classNameList = new ArrayList();
			for (int i = 0; i < split.length; i++) {
				String path = split[i];
				File file = new File(path);
				URI uri = file.toURI();
				url[i] = uri.toURL();
				String[] classNames = ClassFinder.getAllClassNames(path);
				for (int j = 0; j < classNames.length; j++) {
					classNameList.add(classNames[j]);
				}
			}
			URLClassLoader cl = new URLClassLoader(url);
			for (String className : classNameList) {
				Class<?> cls = Class.forName(className, true, cl);
				
				// KeyType registration
				if (KeyType.class.isAssignableFrom(cls) && 
					cls.getSimpleName().matches(".*_v\\d++$") == false) 
				{
					Method method = cls.getMethod("getKeyType", (Class[])null);
					KeyType keyType = (KeyType)method.invoke(cls, (Object[])null);
					KeyTypeManager.registerKeyType(keyType);
				}
			}
      gfsh.println();
      gfsh.println("application classes successfully loaded from: " + jarPaths);
		} catch (Exception ex) {
			gfsh.println("Error: " + ex.getClass().getSimpleName() + " - " + ex.getMessage());
			if (gfsh.isDebug()) {
				ex.printStackTrace();
			}
		}
	}
	
	private void class_dir(String command)
	{
		ArrayList list = new ArrayList();
		gfsh.parseCommand(command, list);
		if (list.size() < 3) {
			gfsh.println("Error: must specify the directory path");
			return;
		}
		
		String dirPath = (String)list.get(2);
		if (dirPath == null) {
			return;
		}
		dirPath = dirPath.trim();
		if (dirPath.length() == 0) {
			return;
		}
		
		File file = new File(dirPath);
		if (file.exists() == false) {
			return;
		}
		
		ArrayList<File> jarFileList = getJarFiles(file);
		try {
			ArrayList<String> classNameList = new ArrayList<String>();
			URL url[] = new URL[jarFileList.size()];
			int i = 0;
			for (File file2 : jarFileList) {
				URI uri = file2.toURI();
				url[i++] = uri.toURL();
				String[] classNames = ClassFinder.getAllClassNames(file2.getAbsolutePath());
				for (int j = 0; j < classNames.length; j++) {
					classNameList.add(classNames[j]);
				}
			}
			URLClassLoader cl = new URLClassLoader(url);
			for (String className : classNameList) {
				Class.forName(className, true, cl);
			}
      gfsh.println();
			gfsh.println("application classes successfully loaded from: " + dirPath);
		} catch (Exception ex) {
			gfsh.println("Error: " + ex.getClass().getSimpleName() + " - " + ex.getMessage());
			if (gfsh.isDebug()) {
				ex.printStackTrace();
			}
		}
	}
	
	private ArrayList<File> getJarFiles(File dir)
	{
		return getJarFiles(dir, new ArrayList<File>());
	}
	
	private ArrayList<File> getJarFiles(File dir, ArrayList<File> fileList)
	{
		File files[] = dir.listFiles(new FileFilter() {

			public boolean accept(File pathname)
			{
				return (pathname.isDirectory() == false && pathname.getName().endsWith(".jar"));
			}
		});
		
		for (int i = 0; i < files.length; i++) {
			fileList.add(files[i]);
		}
		
		File dirs[] = dir.listFiles(new FileFilter() {

			public boolean accept(File pathname)
			{
				return pathname.isDirectory();
			}
		});
		
		for (int i = 0; i < dirs.length; i++) {
			fileList = getJarFiles(dirs[i], fileList);
		}
		
		return fileList;
	}
}
