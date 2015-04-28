package com.gemstone.gemfire.internal.tools.gfsh.app;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.PrintStream;
import java.io.Reader;
import java.io.StringReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Stack;
import java.util.TreeMap;
import java.util.TreeSet;

import jline.ArgumentCompletor;
import jline.ConsoleReader;
import jline.History;
import jline.SimpleCompletor;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.Instantiator;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolFactory;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.cache.query.types.CollectionType;
import com.gemstone.gemfire.cache.query.types.ObjectType;
import com.gemstone.gemfire.cache.query.types.StructType;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.internal.tools.gfsh.app.aggregator.Aggregator;
import com.gemstone.gemfire.internal.tools.gfsh.app.cache.CacheBase;
import com.gemstone.gemfire.internal.tools.gfsh.app.cache.InstantiatorClassLoader;
import com.gemstone.gemfire.internal.tools.gfsh.app.cache.data.GenericMessage;
import com.gemstone.gemfire.internal.tools.gfsh.app.cache.data.ListMap;
import com.gemstone.gemfire.internal.tools.gfsh.app.cache.data.ListMapMessage;
import com.gemstone.gemfire.internal.tools.gfsh.app.cache.data.ListMessage;
import com.gemstone.gemfire.internal.tools.gfsh.app.cache.data.MapMessage;
import com.gemstone.gemfire.internal.tools.gfsh.app.cache.data.Mappable;
import com.gemstone.gemfire.internal.tools.gfsh.app.cache.index.EntryMap;
import com.gemstone.gemfire.internal.tools.gfsh.app.cache.index.IndexInfo;
import com.gemstone.gemfire.internal.tools.gfsh.app.cache.index.LookupService;
import com.gemstone.gemfire.internal.tools.gfsh.app.cache.index.task.ForceGCTask;
import com.gemstone.gemfire.internal.tools.gfsh.app.cache.index.task.IndexInfoTask;
import com.gemstone.gemfire.internal.tools.gfsh.app.cache.index.task.QuerySizeTask;
import com.gemstone.gemfire.internal.tools.gfsh.app.cache.index.task.QueryTask;
import com.gemstone.gemfire.internal.tools.gfsh.app.command.CommandClient;
import com.gemstone.gemfire.internal.tools.gfsh.app.command.task.EchoTask;
import com.gemstone.gemfire.internal.tools.gfsh.app.command.task.PartitionedRegionAttributeTask;
import com.gemstone.gemfire.internal.tools.gfsh.app.command.task.RefreshAggregatorRegionTask;
import com.gemstone.gemfire.internal.tools.gfsh.app.command.task.RegionClearTask;
import com.gemstone.gemfire.internal.tools.gfsh.app.command.task.RegionCreateTask;
import com.gemstone.gemfire.internal.tools.gfsh.app.command.task.RegionDestroyTask;
import com.gemstone.gemfire.internal.tools.gfsh.app.command.task.RegionPathTask;
import com.gemstone.gemfire.internal.tools.gfsh.app.command.task.RegionSizeTask;
import com.gemstone.gemfire.internal.tools.gfsh.app.command.task.data.MemberInfo;
import com.gemstone.gemfire.internal.tools.gfsh.app.command.task.data.PartitionAttributeInfo;
import com.gemstone.gemfire.internal.tools.gfsh.app.command.task.data.RegionAttributeInfo;
import com.gemstone.gemfire.internal.tools.gfsh.app.misc.util.ClassFinder;
import com.gemstone.gemfire.internal.tools.gfsh.app.misc.util.ReflectionUtil;
import com.gemstone.gemfire.internal.tools.gfsh.app.misc.util.StringUtil;
import com.gemstone.gemfire.internal.tools.gfsh.app.pogo.MapLite;
import com.gemstone.gemfire.internal.tools.gfsh.app.util.ObjectUtil;
import com.gemstone.gemfire.internal.tools.gfsh.app.util.PrintUtil;
import com.gemstone.gemfire.internal.tools.gfsh.app.util.SimplePrintUtil;
import com.gemstone.gemfire.internal.tools.gfsh.command.CommandResults;

public class Gfsh extends CacheBase
{

	public final static String PROPERTY_GFSH_INIT_FILE = "gfshInitFile";
	public final static String PROPERTY_DEFAULT_DATA_SERIALIZABLES_FILE = "gfsh.dataSerialiables.file";
	public final static String PROPERTY_COMMAND_JAR_PATH = "gfsh.command.jar.path";
	public final static String PROPERTY_COMMAND_OPT_JAR_PATH = "gfsh.command.opt.jar.path";
	public final static String PROPERTY_USER_COMMAND_JAR_PATHS = "gfsh.user.command.jar.paths";
	public final static String PROPERTY_USER_COMMAND_PACKAGES = "gfsh.user.command.packages";
	public final static String PROPERTY_PLUGIN_JARS = "gfsh.plugin.jars";
    private final static String PROMPT = "gfsh:";
    private final static Set<String> DISABLED_COMMANDS = new HashSet<String>();
	
	private boolean debug = Boolean.getBoolean("gfsh.debug");
	
	private String startupDir = System.getProperty("gfsh.pwd"); 
	
	// User specified arguments. These override .gfshrc.
	private String locators;
	private String servers;
	private String inputFilePath;
	private String dataSerializableClassNames;
	private String dataSerializablesFilePath;
	private String jarDirectoryPath;
	private String jarPaths;
	private TreeSet<String> enumCommandSet = new TreeSet<String>();
	private boolean isAdvancedMode;
	private static final String[] advancedCommands = new String[] {"bcp", "class", "db", "deploy", "gc", "rebalance"/*, "zone"*/};

	// Contains all of the supported commands (commandName, CommandExecutable)
	private HashMap commandMap = new HashMap();

	// All fields are initialized in postInit().
	private boolean echo = false;
	private long zoneDifference = 0;
	private boolean showTime = true;
	private boolean showResults;
	private String queryKeyClassName;
	private Class<?> queryKeyClass;
	private String valueClassName;
	private Class<?> valueClass;
	private LookupService lookupService;
	private String endpoints = "localhost:40401";
	private int readTimeout = 300000; // 300 sec or 5 min
	private boolean isLocator = false;
	private String serverGroup = null;
	private Pool pool;
	
	private Properties envProperties = new Properties();

	private CommandClient commandClient;
	private String commandRegionPath = "/__command";
	private String aggregateRegionPath = commandRegionPath + "/pr";
	private Aggregator aggregator;
	private String currentPath = "/";
	private Region<?, ?> currentRegion;
	private List<?> lsKeyList;
	private int selectLimit;
	private int fetchSize = 100;
	private boolean tableFormat = false;
	private boolean printType = true;
	private int collectionEntryPrintCount;

	private SimpleDateFormat dateFormat = new SimpleDateFormat();

	// jline reader
	private ConsoleReader consoleReader;
	
	private BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));

	private String[] commands;
	
	static {
	  DISABLED_COMMANDS.add("look");
	  DISABLED_COMMANDS.add("perf");
	  DISABLED_COMMANDS.add("zone");
	}

  public Gfsh(String args[]) throws Exception {
    // parse user provide args
    parseArgs(args);
    
    println();
    
    initCommands();
    initJline();

    initializeLogStatsResources();
    // Initialize the cache
    initializeCache();

    setEcho(false);
    setShowResults(true);
    setShowTime(true);

    postInit(args);
    
    println();
    
    if (isConnected() == false) {
      println("Warning: not connected. Use the 'connect' command to connect to");
      println("         locator(s) or cache server(s).");
      println();
    }
  }
	
	protected void initializeCache() throws CacheException, IOException
	{
		// Load the default DataSerializable objects required by the utility packages.
		try {
      // gfaddon-util package - The base id is 200      
      // gfcommand package - The base id is 300      
      // MapLite registration
      Class.forName("com.gemstone.gemfire.internal.tools.gfsh.app.DataSerializablesInitializer");
			
		} catch (ClassNotFoundException ex) {
			println("Error: ClassNotFoundException. Unable to load the utility classes - " + ex.getMessage());
			if (isDebug()) {
				ex.printStackTrace();
			}
		}
		
		// Load the default DataSerializable file
		try {
			String defaultDataSerializablesFilePath = System.getProperty(PROPERTY_DEFAULT_DATA_SERIALIZABLES_FILE, "etc/DataSerializables.txt");
			InstantiatorClassLoader.loadDataSerializables(defaultDataSerializablesFilePath);
		} catch (IOException e) {
			// ignore
		} catch (ClassNotFoundException ex) {
			println("Error: ClassNotFoundException. Unabled to load class - " + ex.getMessage());
			if (isDebug()) {
				ex.printStackTrace();
			}
		}
	}
	
	private boolean isImplement(Class cls, Class interf)
	{
		Class interfaces[] = cls.getInterfaces();
		for (int i = 0; i < interfaces.length; i++) {
			if (interfaces[i] == interf) {
				return true;
			}
		}
		return false;
	}
	
	private void loadCommands(String packageName, String commandPackageName, HashSet<Class> classSet) throws Exception
	{
		Class classes[] = ClassFinder.getClasses(commandPackageName);
		if (classes.length == 0) {
			String jarPath = System.getProperty(packageName);
			classes = ClassFinder.getClasses(jarPath, commandPackageName);
		}
		
		List<String> commands = Arrays.asList(advancedCommands);
		
		for (int i = 0; i < classes.length; i++) {
			if (isImplement(classes[i], CommandExecutable.class)) { 
			  String commandName = classes[i].getSimpleName();
			  if ( !(!isAdvancedMode && commands.contains(commandName)) 
					  && !DISABLED_COMMANDS.contains(commandName) ) {
			    classSet.add(classes[i]);
              }
			}
		}
	}
	
	private void loadPlugins()
	{
		String pluginJars = System.getProperty(PROPERTY_PLUGIN_JARS);
		if (pluginJars == null) {
			return;
		}
		pluginJars = pluginJars.trim();
		if (pluginJars.length() == 0) {
			return;
		}
		
		String pathSeparator = System.getProperty("path.separator");
		String split[] = pluginJars.split(pathSeparator);
		for (int i = 0; i < split.length; i++) {
			try {
				ClassFinder.getAllClasses(split[i]);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	private void initCommands() throws Exception
	{
		// Read all core command classes found in the package
		// com.gemstone.gemfire.internal.tools.gfsh.commands
		HashSet<Class> classSet = new HashSet<Class>();
		loadCommands(PROPERTY_COMMAND_JAR_PATH, "com.gemstone.gemfire.internal.tools.gfsh.app.commands", classSet);
		
		// Read all optional command classes found in the package
		// com.gemstone.gemfire.internal.tools.gfsh.commands.optional
//		boolean optEnabled = Boolean.getBoolean("gfsh.opt.command.enabled");
//		if (optEnabled) {
//			loadCommands(PROPERTY_COMMAND_OPT_JAR_PATH, "com.gemstone.gemfire.internal.tools.gfsh.app.commands.optional", classSet);
//		}
		
		// Read user specified command classes
		String userPackageNames = System.getProperty(PROPERTY_USER_COMMAND_PACKAGES);
		if (userPackageNames != null) {
			userPackageNames = userPackageNames.trim();
			String packageSplit[] = null;
			if (userPackageNames.length() > 0) {
				packageSplit = userPackageNames.split(",");
				for (int i = 0; i < packageSplit.length; i++) {
					Class[] classes2 = ClassFinder.getClasses(packageSplit[i]);			
					for (int j = 0; j < classes2.length; j++) {
						if (isImplement(classes2[j], CommandExecutable.class)) { 
							classSet.add(classes2[j]);
						}
					}
				}
			}

			String jarPaths = System.getProperty(PROPERTY_USER_COMMAND_JAR_PATHS);
			if (jarPaths != null && packageSplit != null) {
				jarPaths = jarPaths.trim();
				if (jarPaths.length() > 0) {
					String split[] = jarPaths.split(",");
					for (int i = 0; i < split.length; i++) {
						for (int k = 0; k < packageSplit.length; k++) {
							Class[] classes2 = ClassFinder.getClasses(split[i], packageSplit[k]);
							for (int j = 0; j < classes2.length; j++) {
								if (isImplement(classes2[j], CommandExecutable.class)) { 
									classSet.add(classes2[j]);
								}
							}
						}
					}
				}
			}
		}

		ArrayList<String> commandList = new ArrayList();
		for (Class class1 : classSet) {
			commandList.add(getCommandName(class1));
		}
		Collections.sort(commandList);
		commands = commandList.toArray(new String[0]);

		for (Class commandClass : classSet) {
			Constructor constructor = commandClass.getConstructor(this.getClass());
			Object commandObject = constructor.newInstance(this);
			commandMap.put(getCommandName(commandObject.getClass()), commandObject);
		}
	}
	
	private String getCommandName(Class class1)
	{
		String name = class1.getSimpleName();
		if (name.equals("classloader")) {
			name = "class";
		}
		return name;
	}

	private void initFile(String relativeFilePath) throws IOException
	{
		// Load data classes
		if (dataSerializableClassNames != null) {
			String split[] = dataSerializableClassNames.split(",");
			for (int i = 0; i < split.length; i++) {
				try {
					Class clazz = Class.forName(split[i]);
					println("Loaded " + clazz.getName());
				} catch (ClassNotFoundException e) {
					println(split[i] + " - " + e.getClass().getSimpleName() + ": " + e.getMessage());
					if (isDebug()) {
						e.printStackTrace();
					}
				}
			}
		}
		
		// Load data classes listed in DataSerializables.txt
		File dataSerializablesFile = null;
		if (dataSerializablesFilePath != null) {
			if (dataSerializablesFilePath.startsWith("/") || dataSerializablesFilePath.indexOf(':') >= 0) {
				// absolute path
				dataSerializablesFile = new File(dataSerializablesFilePath);
			} else {
				// relative path
				if (startupDir != null) {
					dataSerializablesFile = new File(startupDir, dataSerializablesFilePath);
				} else {
					dataSerializablesFile = new File(dataSerializablesFilePath);
				}
			}
		}
		
		if (dataSerializablesFile != null) {
			if (dataSerializablesFile.exists() == false) {
				println();
				println("Error: specified file does not exist: " + dataSerializablesFile.getAbsolutePath());
				println();
				System.exit(-1);
			}
			
			execute("class -d " + dataSerializablesFile.getAbsolutePath());
		}
		
		
		// Load jars in directory
		if (jarDirectoryPath != null) {
			File file = new File(jarDirectoryPath);
			if (file.exists() == false) {
				println();
				println("Error: specified file does not exist: " + jarDirectoryPath);
				println();
				System.exit(-1);
			}
			execute("class -dir " + jarDirectoryPath);
		}
		
		// Load jars
		if (jarPaths != null) {
			execute("class -jar " + jarPaths);
		}
		
		// Load all plugins - DataSerializables.txt not needed if 
		// the data class jar files are placed in the addons/plugins
		// directory. Note that loadPlugins() loads all classes
		// in that directory including subdirectories.
		loadPlugins();

		
		// Get .gfshrc file
		File gfshrcFile = null;
		if (inputFilePath != null) {
			if (inputFilePath.startsWith("/") || inputFilePath.indexOf(':') >= 0) {
				// absolute path
				gfshrcFile = new File(inputFilePath);
			} else {
				// relative path
				if (startupDir != null) {
					gfshrcFile = new File(startupDir, inputFilePath);
				} else {
					gfshrcFile = new File(inputFilePath);
				}
			}
		}
		
		String userHomeDir = System.getProperty("user.home");
		
		// if the input file is valid
		if (gfshrcFile != null) {
			if (gfshrcFile.exists() == false || gfshrcFile.isFile() == false) {
				println();
				println("Error: invalid input file - " + inputFilePath);
				println();
				System.exit(-1);
			} 
		} else {
			gfshrcFile = new File(userHomeDir, relativeFilePath);
			if (gfshrcFile.exists() == false) {
				gfshrcFile = new File(userHomeDir, ".gfshrc");
				if (gfshrcFile.exists() == false) {
					gfshrcFile.createNewFile();
				}
			}
		}
		
		File gfshDir = new File(userHomeDir, ".gemfire");
		if (gfshDir.exists() == false) {
			gfshDir.mkdir();
		}
		
		File etcDir = new File(gfshDir, "etc");
		if (etcDir.exists() == false) {
			etcDir.mkdir();
		}
//		File logDir = new File(gfshDir, "log");
//		if (logDir.exists() == false) {
//			logDir.mkdir();
//		}
//		File statsDir = new File(gfshDir, "stats");
//		if (statsDir.exists() == false) {
//			statsDir.mkdir();
//		}

		LineNumberReader reader = new LineNumberReader(new FileReader(gfshrcFile));
		String line = reader.readLine();
		String command;
		ArrayList<String> commandList = new ArrayList();
		StringBuffer buffer = new StringBuffer();
		while (line != null) {
			command = line.trim();
			if (command.length() > 0 && command.startsWith("#") == false) {
				if (command.endsWith("\\")) {
					buffer.append(command.substring(0, command.length() - 1));
				} else {
					buffer.append(command);
					commandList.add(buffer.toString().trim());
					buffer = new StringBuffer();
				}
			}
			line = reader.readLine();
		}
		reader.close();

		// Connect first if locators or servers are specified
		command = null;
		if (locators != null) {
			command = "connect -l " + locators;
		} else if (servers != null) {
			command = "connect -s " + servers;
		} else {
		  command = "connect -s localhost:"+CacheServer.DEFAULT_PORT;
		  println("Connecting using defaults: -s localhost:"+CacheServer.DEFAULT_PORT);
		}
		if (command != null) {
			execute(command);
		}
		
		// Execute commands
		String commandName = null;
		for (int i = 0; i < commandList.size(); i++) {
			command = commandList.get(i);
			command = expandProperties(command);
			
			if (isEcho()) {
				println(command);
			}
			
			// if locators or servers are specified then override
			// the connect command.
//			if (locators != null || servers != null) {
//				if (command.startsWith("connect") && 
//						(command.indexOf("-l") >= 0 || command.indexOf("-s") >= 0)) 
//				{
//					String newCommand = "connect";
//					
//					// keep non-connection options
//					ArrayList<String> list = new ArrayList();
//					parseCommand(command, list);
//					for (int j = 1; j < list.size(); j++) {
//						String token = list.get(i);
//						if (token.equals("-s")) {
//							j++;
//						} else if (token.equals("-l")) {
//							j++;
//						} else if (token.equals("-r")) {
//							j++;
//							newCommand = newCommand + " -r " + list.get(j);
//						}
//					}
//					
//					if (newCommand.length() > "connect".length()) {
//						execute(newCommand);
//					}
//				}
//			} else {
				execute(command);
//			}
		}
	}
	
	public void setDebug(boolean debug)
	{
		this.debug = debug;
	}
	
	public boolean isDebug()
	{
		return debug;
	}
	
	public void execute(String command)
	{
		String commandName = null;
		if (command != null) {
			command = command.trim();
			String[] split = command.split(" ");
			commandName = split[0];
		}

		if (command == null /* EOF */|| command.startsWith("exit") || command.startsWith("quit")) {
			close();
			System.exit(0);
		}

		CommandExecutable executable = getCommand(commandName);
		if (executable == null) {
			println("Error: undefined command: " + commandName);
		} else {
			try {
				executable.execute(command);
			} catch (Exception ex) {
        getCache().getLogger().error("While executing '"+command+"'", ex);
				println("Error: " + command + " -- " + getCauseMessage(ex));
			}
		}
	}

	public CommandExecutable getCommand(String commandName)
	{
		if (commandName.equals("n")) {
			commandName = "next";
		} else if (commandName.equals("class")) {
			
		}
		return (CommandExecutable) commandMap.get(commandName);
	}
	
	public String getEnumCommands()
	{
		return enumCommandSet.toString();
	}

	public void addEnumCommand(String command)
	{
		enumCommandSet.add(command);
	}
	
	private void postInit(String args[])
	{
		String iniFilePath = System.getProperty(PROPERTY_GFSH_INIT_FILE, ".gfshrc");
		
		zoneDifference = 0;
		endpoints = null;
		queryKeyClassName = null;
		fetchSize = 100;
		showResults = true;
		showTime = true;
		setCollectionEntryPrintCount(5);
		setPrintType(true);
		setTableFormat(true);

		try {
			initFile(iniFilePath);
		} catch (IOException ex) {
			println("Error: reading file " + iniFilePath + " -- " + getCauseMessage(ex));
			if (isDebug()) {
				ex.printStackTrace();
			}
		}
		
		// If the initialization did not create the cache then create it.
		// No connection to the server but only local cache.
		if (cache == null) {
			try {
				open();
			} catch (IOException ex) {
				println("Error: unable to create cache - " + ex.getMessage());
				if (isDebug()) {
					ex.printStackTrace();
				}
			}
		}
	}
	
	public String getLine(String prompt) throws IOException
	{
    if (prompt == null) {
      prompt = PROMPT + currentPath + ">";
    }

		StringBuffer cmdBuffer = null;
		boolean keepGoing;
		String nextLine;
		do {
			keepGoing = false;
      if (consoleReader == null) {
        print(prompt);
        nextLine = bufferedReader.readLine();
      } else {
        nextLine = consoleReader.readLine(prompt);
      }

			// if nextLine is null then we encountered EOF.
			// In that case, leave cmdBuffer null if it is still null

			if (this.isEcho()) {
				if (nextLine == null) {
					println("EOF");
				} else if (nextLine.length() != 0) {
//        println(nextLine); //FIXME: echo here not required? it echoes twice
				}
			}

			if (nextLine == null) {
				break;
			} else if (cmdBuffer == null) {
				cmdBuffer = new StringBuffer();
			}

			// if last character is a backward slash, replace backward slash
			// with LF and continue to next line
			if (nextLine.endsWith("\\")) {
				nextLine = nextLine.substring(0, nextLine.length() - 1);
				keepGoing = true;
			}
			cmdBuffer.append(nextLine);
//			if (keepGoing) {
//				cmdBuffer.append('\n');
//			}
		} while (keepGoing);

//		if (this.isEcho())
//			println();

		return cmdBuffer == null ? null : cmdBuffer.toString();
	}

//FindBugs - private method never called
//	private String getLine(BufferedReader bin) throws IOException
//	{
//		String prompt = currentPath;
//
//		StringBuffer cmdBuffer = null;
//		boolean keepGoing;
//		String nextLine;
//		do {
//			keepGoing = false;
//			if (consoleReader == null) {
//				print(prompt + "> ");
//				nextLine = bin.readLine();
//			} else {
//				nextLine = consoleReader.readLine(prompt + "> ");
//			}
//
//			// if nextLine is null then we encountered EOF.
//			// In that case, leave cmdBuffer null if it is still null
//
//			if (this.echo) {
//				if (nextLine == null) {
//					println("EOF");
//				} else if (nextLine.length() != 0) {
//					println(nextLine);
//				}
//			}
//
//			if (nextLine == null) {
//				break;
//			} else if (cmdBuffer == null) {
//				cmdBuffer = new StringBuffer();
//			}
//
//			// if last character is a backward slash, replace backward slash
//			// with
//			// LF and continue to next line
//			if (nextLine.endsWith("\\")) {
//				nextLine = nextLine.substring(0, nextLine.length() - 1);
//				keepGoing = true;
//			}
//			cmdBuffer.append(nextLine);
//			if (keepGoing) {
//				cmdBuffer.append('\n');
//			}
//		} while (keepGoing);
//
//		if (this.echo)
//			println();
//
//		return cmdBuffer == null ? null : cmdBuffer.toString();
//	}

	/**
	 * Returns a data object for the specified function call
	 * @param value The data function of the formant "to_date('date', 'simple date formant')"
	 * @throws ParseException
	 */
	public Date getDate(String value) throws ParseException
	{
		Date date = null;

		// to_date('10/10/2008', 'MM/dd/yyyy')
		String lowercase = value.toLowerCase();
		boolean error = false;
		if (lowercase.startsWith("to_date") == false) {
			error = true;
		} else {
			int index = value.indexOf('(');
			if (index == -1) {
				error = true;
			}
			value = value.substring(index + 1);
			String split2[] = value.split(",");
			if (split2.length != 2) {
				error = true;
			} else {
				for (int j = 0; j < split2.length; j++) {
					split2[j] = split2[j].trim();
				}
				String dateStr = StringUtil.trim(split2[0], '\'');
				String format = StringUtil.trim(StringUtil.trimRight(split2[1], ')'), '\'');
				dateFormat.applyPattern(format);
				date = dateFormat.parse(dateStr);
			}
		}
		if (error) {
			println("   Invalid date macro. Must use to_date('<date>', '<format>'). Ex, to_date('10/10/08', 'MM/dd/yy')");
		}
		return date;
	}

	public void println(Object obj)
	{
		System.out.println(obj);
	}

	public void println()
	{
		System.out.println();
	}

	public void print(String s)
	{
		System.out.print(s);
	}

	/**
	 * Prints information on how this program should be used.
	 */
	public void showHelp()
	{
		PrintStream out = System.out;
		out.println();
		out.println("Commands:");
		out.println();

		for (int i = 0; i < commands.length; i++) {
			CommandExecutable exe = getCommand(commands[i]);
			exe.help();
		}

		out.println("exit or quit");
		out.println("     Close the current cache and exits");
		out.println();

	}
	
	 public void showHelp(String command)
	  {
	    PrintStream out = System.out;
      CommandExecutable exe = getCommand(command);
      if (exe != null) {
        exe.help();
      } else {
        out.println("Could not find command: "+command);
      }
	    out.println();
	  }
	
	private TreeMap<String, Mappable> memberMap = new TreeMap<String, Mappable>();
	
	public List<Mappable> setMemberList(List<Mappable> memberMapList)
	{
		memberMap.clear();
		for (Mappable mappable : memberMapList) {
			String memberId;
			try {
				memberId = mappable.getString("MemberId");
				memberMap.put(memberId, mappable);
			} catch (Exception ex) {
				// ignore
			}
		}
		ArrayList<Mappable> sortedList = new ArrayList(memberMap.values());
		return sortedList;
	}
	
	public String getMemberId(int memberNumber)
	{
		Set<String> set = memberMap.keySet();
		int index = memberNumber - 1;
		if (set.size() > index) {
			int i = 0;
			for (String memberId : set) {
				if (index == i) {
					return memberId;
				}
			}
		}
		return null;
	}

	private void initJline() throws Exception
	{
//		String osName = System.getProperty("os.name");
//		if (osName.startsWith("Windows")) {
//			return;
//		}

		consoleReader = new ConsoleReader();
		consoleReader.setBellEnabled(false);
//		consoleReader.setDebug(new java.io.PrintWriter("jline-debug.txt"));
		History history = consoleReader.getHistory();
		if (history == null) {
		  history = new History();
		  consoleReader.setHistory(history);
		}
		File historyFile = new File(System.getProperty("user.home"), ".gfshhistory");
		history.setHistoryFile(historyFile);
//		reader.setDebug(new PrintWriter(new FileWriter("writer.debug", true)));

		List completors = new LinkedList();
		completors.add(new SimpleCompletor(commands));
		consoleReader.addCompletor(new ArgumentCompletor(completors));
	}

	/**
	 * Prompts the user for input and executes the command accordingly.
	 */
	void go()
	{
		println();
		println("Enter 'help' or '?' for help at the command prompt.");
		println("");
//		BufferedReader bin = new BufferedReader(new InputStreamReader(System.in));
		String[] split;
		String commandName = null;

		while (true) {
			try {
				println();
				String command = getLine((String)null);
				println();
				if (command != null) {
					command = command.trim();
					command = expandProperties(command);
					if (isEcho()) {
						println(command);
					}
					split = command.split(" ");
					commandName = split[0];
				}

				if (command == null /* EOF */|| command.startsWith("exit") || command.startsWith("quit")) {
					close();
					System.exit(0);
				}

				// Execute the command found in the map
				CommandExecutable executable = getCommand(commandName);
				if (executable != null) {
					executable.execute(command);
					continue;
				}

				// Some command name exceptions...
				if (command.matches("\\(.*select.*")) {
					executable = getCommand("select");
				} else if (command.startsWith("?")) {
					executable = getCommand("help");
				}
				if (executable != null) {
					executable.execute(command);
					continue;
				}

				// the specified command not supported
				if (command.length() != 0) {
					println("Unrecognized command. Enter 'help' or '?' to get a list of commands.");
				}

			} catch (Exception ex) {
				println("Error: " + getCauseMessage(ex));
//				cache.getLogger().info(ex);
				if (debug) {
					ex.printStackTrace();
				}
			}
		}
	}

	public String getCauseMessage(Throwable ex)
	{
		Throwable cause = ex.getCause();
		String causeMessage = null;
		if (cause != null) {
			causeMessage = getCauseMessage(cause);
		} else {
			causeMessage = ex.getClass().getSimpleName();
			causeMessage += " -- " + ex.getMessage();
		}
		return causeMessage;
	}

	/**
	 * Parses a <code>command</code> and places each of its tokens in a
	 * <code>List</code>. Tokens are separated by whitespace, or can be wrapped
	 * with double-quotes
	 */
	public static boolean parseCommand(String command, List list)
	{
		Reader in = new StringReader(command);
		StringBuffer currToken = new StringBuffer();
		String delim = " \t\n\r\f";
		int c;
		boolean inQuotes = false;
		do {
			try {
				c = in.read();
			} catch (IOException e) {
				throw new Error("unexpected exception", e);
			}

			if (c < 0)
				break;

			if (c == '"') {
				if (inQuotes) {
					inQuotes = false;
					list.add(currToken.toString().trim());
					currToken = new StringBuffer();
				} else {
					inQuotes = true;
				}
				continue;
			}

			if (inQuotes) {
				currToken.append((char) c);
				continue;
			}

			if (delim.indexOf((char) c) >= 0) {
				// whitespace
				if (currToken.length() > 0) {
					list.add(currToken.toString().trim());
					currToken = new StringBuffer();
				}
				continue;
			}

			currToken.append((char) c);
		} while (true);

		if (currToken.length() > 0) {
			list.add(currToken.toString().trim());
		}
		return true;
	}
	
	public Object getKeyFromKeyList(int keyNum)
	{
		int index = keyNum - 1;
		if (getLsKeyList() == null || getLsKeyList().size() <= index) {
			return null;
		}
		return getLsKeyList().get(index);
	}
	
	public Map getKeyMap(List list, int startIndex)
	{
		HashMap keyMap = new HashMap();
		for (int i = startIndex; i < list.size(); i++) {
			String val = (String)list.get(i);
			String split[] = val.split("-"); 
			if (split.length == 2) {
				int startI = Integer.parseInt(split[0]);
				int endIndex = Integer.parseInt(split[1]);
				if (endIndex > getLsKeyList().size()) {
					if (getLsKeyList().size() == 0) {
						println("Error: Key list empty.");
					} else {
						println("Error: Out of range. Valid range: 1-" + getLsKeyList().size());
					}
					return keyMap;
				}
				for (int j = startI; j <= endIndex; j++) {
					Object key = getLsKeyList().get(j-1);
					keyMap.put(j, key);
				}	
			} else {
				int index = Integer.parseInt(split[0]);
				Object key = getLsKeyList().get(index-1);
				keyMap.put(index, key);
			}
		}
		return keyMap;
	}

	public Object getQueryKey(List list, int startIndex) throws Exception
	{
		// See if key is a primitive
		String input = (String) list.get(startIndex);
		Object key = null;
		if (input.startsWith("'")) {
			int lastIndex = -1;
			if (input.endsWith("'") == false) {
				lastIndex = input.length();
			} else {
				lastIndex = input.lastIndexOf("'");
			}
			if (lastIndex <= 1) {
				println("Error: Invalid key. Empty string not allowed.");
				return null;
			}
			key = input.subSequence(1, lastIndex); // lastIndex exclusive
		} else {
			key = ObjectUtil.getPrimitive(this, input, false);
		}
		if (key != null) {
			return key;
		}
		
		
		// Key is an object
		
		// query key class must be defined
		if (queryKeyClass == null) {
			println("Error: key undefined. Use the key command to specify the key class.");
			return null;
		}
		
		// f1=v1 and f2='v2' and f3=v3

		// Build the query predicate from the argument list
		String queryPredicate = "";
		for (int i = startIndex; i < list.size(); i++) {
			queryPredicate += list.get(i) + " ";
		}
		String[] split = queryPredicate.split("and");

		// Create the query key by invoking setters for each
		// parameter listed in the queryPredicate
		Object queryKey = queryKeyClass.newInstance();
		Map<String, Method> setterMap = ReflectionUtil.getAllSettersMap(queryKey.getClass());
		for (int i = 0; i < split.length; i++) {
			String token = split[i];
			String[] tokenSplit = token.split("=");
			if (tokenSplit.length < 2) {
				println("Error: Invalid query: " + token);
				return null;
			}
			String field = tokenSplit[0].trim();
			String value = tokenSplit[1].trim();
			String setterMethodName = "set" + field;

			Method setterMethod = setterMap.get(setterMethodName);
			if (setterMethod == null) {
				println("Error: " + setterMethodName + " undefined in " + queryKeyClass.getName());
				return null;
			}
			Class types[] = setterMethod.getParameterTypes();
			Class arg = types[0];
			if (arg == byte.class || arg == Byte.class) {
				setterMethod.invoke(queryKey, Byte.parseByte(value));
			} else if (arg == char.class || arg == Character.class) {
				setterMethod.invoke(queryKey, value.charAt(0));
			} else if (arg == short.class || arg == Short.class) {
				setterMethod.invoke(queryKey, Short.parseShort(value));
			} else if (arg == int.class || arg == Integer.class) {
				setterMethod.invoke(queryKey, Integer.parseInt(value));
			} else if (arg == long.class || arg == Long.class) {
				setterMethod.invoke(queryKey, Long.parseLong(value));
			} else if (arg == float.class || arg == Float.class) {
				setterMethod.invoke(queryKey, Float.parseFloat(value));
			} else if (arg == double.class || arg == Double.class) {
				setterMethod.invoke(queryKey, Double.parseDouble(value));
			} else if (arg == Date.class) {
				Date date = getDate(value);
				if (date == null) {
					println("Error: Unable to parse date.");
					return null;
				} else {
					setterMethod.invoke(queryKey, date);
				}
			} else if (arg == String.class) {
				if (value.startsWith("'")) {
					value = value.substring(1);
					if (value.endsWith("'")) {
						value = value.substring(0, value.length() - 1);
					}
				}
				setterMethod.invoke(queryKey, value);
			} else {
				println("Error: Unsupported type: " + setterMethod.getName() + "(" + arg.getName() + ")");
				return null;
			}
		}

		return queryKey;
	}

	public int printSelectResults(SelectResults sr, int rowCount)
	{
		if (sr == null) {
			println("Error: SelectResults is null");
			return 0;
		}

		StringBuffer sb = new StringBuffer();
		CollectionType type = sr.getCollectionType();
		sb.append(sr.size());
		sb.append(" results in a collection of type ");
		sb.append(type);
		sb.append("\n");

		ObjectType elementType = type.getElementType();
		int row = 1;
		if (rowCount == -1) {
			rowCount = Integer.MAX_VALUE;
		}
		for (Iterator iter = sr.iterator(); iter.hasNext() && row <= rowCount;) {
			Object element = iter.next();
			if (elementType.isStructType()) {
				StructType structType = (StructType) elementType;
				Struct struct = (Struct) element;
				ObjectType[] fieldTypes = structType.getFieldTypes();
				String[] fieldNames = structType.getFieldNames();
				Object[] fieldValues = struct.getFieldValues();

				sb.append("  Struct with ");
				sb.append(fieldTypes.length);
				sb.append(" fields\n");

				for (int i = 0; i < fieldTypes.length; i++) {
					ObjectType fieldType = fieldTypes[i];
					String fieldName = fieldNames[i];
					Object fieldValue = fieldValues[i];

					sb.append("    ");
					sb.append(fieldValue.getClass().getName());
					sb.append(" ");
					if (/*fieldName instanceof String || */element.getClass().isPrimitive()) { //FindBugs - instanceof not needed here
						sb.append(fieldName);
					} else {
						sb.append(ReflectionUtil.toStringGettersAnd(fieldName));
					}
					sb.append(" = ");
					if (fieldValue instanceof String || element.getClass().isPrimitive()) {
						sb.append(fieldValue);
					} else {
						sb.append(ReflectionUtil.toStringGettersAnd(fieldValue));
					}
					sb.append("\n");
				}

			} else {
				sb.append("  ");
				sb.append(row);
				sb.append(". ");
				if (element instanceof String || element.getClass().isPrimitive()) {
					sb.append(element);
				} else {
					sb.append(ReflectionUtil.toStringGettersAnd(element));
				}
			}

			sb.append("\n");
			row++;
		}

		println(sb);
		return row - 1;
	}
	
	public void refreshAggregatorRegion()
	{
		commandClient.execute(new RefreshAggregatorRegionTask());
	}

	public void printEntry(Object key, Object value)
	{
		if (key instanceof String || key.getClass().isPrimitive()) {
			System.out.print(key);
		} else {
			System.out.print(ReflectionUtil.toStringGettersAnd(key));
		}
		System.out.print(" ==> ");
		if (value instanceof String || value.getClass().isPrimitive()) {
			System.out.print(value);
		} else {
			System.out.print(ReflectionUtil.toStringGettersAnd(value));
		}
	}

	/**
	 * Returns the full path. Supports '..'.
	 * 
	 * @param newPath
	 *            The new path to be evaluated
	 * @param currentPath
	 *            The current path
	 * @return Returns null if the new path is invalid.
	 */
	public String getFullPath(String newPath, String currentPath)
	{
		if (newPath == null) {
			return null;
		}
		if (newPath.startsWith("/")) {
			return newPath;
		}
		
		if (currentPath == null) {
			currentPath = "/";
		}

		String path = currentPath;
		String[] split = currentPath.split("/");
		Stack pathStack = new Stack<String>();
		for (int i = 0; i < split.length; i++) {
			if (split[i].length() == 0) {
				continue;
			}
			pathStack.add(split[i]);
		}
		split = newPath.split("/");
		boolean invalidPath = false;
		for (int i = 0; i < split.length; i++) {
			if (split[i].length() == 0) {
				continue;
			}
			String dirName = split[i];
			if (dirName.equals("..")) {
				if (pathStack.size() == 0) {
					invalidPath = true;
					break;
				}
				pathStack.pop();
			} else if (dirName.equals(".")) {
				continue;
			} else {
				pathStack.add(dirName);
			}
		}

		if (invalidPath) {
			return null;
		}

		String fullPath = "";
		while (pathStack.size() > 0) {
			fullPath = "/" + pathStack.pop() + fullPath;
		}
		if (fullPath.length() == 0) {
			fullPath = "/";
		}
		return fullPath;
	}
	
	public void reconnect() throws Exception
	{
		if (commandClient != null) {
			try {
				commandClient.close();
			} catch (Exception ex) {
				// ignore
			}
		}
		if (lookupService != null) {
			try {
				lookupService.close();
			} catch (Exception ex) {
				// ignore
			}
		}
		if (aggregator != null) {
			try {
				aggregator.close();
			} catch (Exception ex) {
				// ignore
			}
		}
		
		// close and reopen the cache
		close();
		open();
		
		PoolFactory factory = PoolManager.createFactory();
		factory.setReadTimeout(readTimeout);
		String split[] = endpoints.split(",");
		for (int i = 0; i < split.length; i++) {
			String locator = split[i];
			String sp2[] = locator.split(":");
			String host = sp2[0];
			int port = Integer.parseInt(sp2[1]);
			if (isLocator) {
				factory.addLocator(host, port);	
			} else {
				factory.addServer(host, port);
			}
			if (serverGroup != null) {
				factory.setServerGroup(serverGroup);
			}
		}
		pool = factory.create("connectionPool");
		commandClient = new CommandClient(commandRegionPath, pool);
		
		lookupService = new LookupService(commandClient);
		aggregator = new Aggregator(commandClient);
    if (logger != null && logger.configEnabled()) {
      logger.config("Available Commands : "+Arrays.toString(commands));
    }
	}
	
	public Pool getPool()
	{
		return pool;
	}

	public boolean isConnected()
	{
		try {
			CommandResults results = commandClient.execute(new EchoTask("hello, world"));
			return true;
		} catch (Exception ex) {
			return false;
		}

	}

	public void close()
	{
		if (cache != null && cache.isClosed() == false) {
			cache.close();
		}
	}

	public boolean isShowResults()
	{
		return showResults;
	}

	public void setShowResults(boolean showResults)
	{
		this.showResults = showResults;
	}

	public String getCurrentPath()
	{
		return currentPath;
	}

	public void setCurrentPath(String regionPath)
	{
		this.currentPath = regionPath;
	}

	public Region getCurrentRegion()
	{
		return currentRegion;
	}

	public void setCurrentRegion(Region region)
	{
		currentRegion = region;
	}

	public boolean isEcho()
	{
		return echo;
	}

	public void setEcho(boolean echo)
	{
		this.echo = echo;
	}

	public long getZoneDifference()
	{
		return zoneDifference;
	}

	public void setZoneDifference(long zoneDifference)
	{
		this.zoneDifference = zoneDifference;
	}

	public String getQueryKeyClassName()
	{
		return queryKeyClassName;
	}

	public void setQueryKeyClassName(String queryKeyClassName)
	{
		this.queryKeyClassName = queryKeyClassName;
	}

	public Class getQueryKeyClass()
	{
		return queryKeyClass;
	}

	public void setQueryKeyClass(Class queryKeyClass)
	{
		this.queryKeyClass = queryKeyClass;
	}
	
	public void setKeyClass(String queryKeyClassName)
	{
		try {
			queryKeyClass = Class.forName(queryKeyClassName);
			this.queryKeyClassName = queryKeyClassName;
		} catch (ClassNotFoundException e) {
			println(e.getMessage());
		}
	}
	
	public String getValueClassName()
	{
		return valueClassName;
	}

	public void setValueClassName(String valueClassName)
	{
		this.valueClassName = valueClassName;
	}
	
	public Class getValueClass()
	{
		return valueClass;
	}

	public void setValueClass(Class valueClass)
	{
		this.valueClass = valueClass;
	}
	
	public void setValueClass(String valueClassName)
	{
		try {
			valueClass = Class.forName(valueClassName);
			this.valueClassName = valueClassName;
		} catch (ClassNotFoundException e) {
			println(e.getMessage());
		}
	}

	public String getEndpoints()
	{
		return endpoints;
	}

	public void setEndpoints(String endpoints, boolean isLocator, String serverGroup, int readTimeout)
	{
		this.endpoints = endpoints;
		this.isLocator = isLocator;
		this.serverGroup = serverGroup;
		this.readTimeout = readTimeout;
	}
	
	public String getServerGroup()
	{
		return serverGroup;
	}
	
	public int getReadTimeout()
	{
		return readTimeout;
	}
	
	public String getCommandRegionPath()
	{
		return commandRegionPath;
	}

	public void setCommandRegionPath(String commandRegionPath)
	{
		this.commandRegionPath = commandRegionPath;
		setAggregateRegionPath(commandRegionPath + "/pr");
	}

	public String getAggregateRegionPath()
	{
		return aggregateRegionPath;
	}

	public void setAggregateRegionPath(String aggregateRegionPath)
	{
		this.aggregateRegionPath = aggregateRegionPath;
	}

	public Aggregator getAggregator()
	{
		return aggregator;
	}

	public void setAggregator(Aggregator aggregator)
	{
		this.aggregator = aggregator;
	}
	
	public int getSelectLimit()
	{
		return selectLimit;
	}
	
	public void setSelectLimit(int selectLimit)
	{
		this.selectLimit = selectLimit;
	}
	
	public int getFetchSize()
	{
		return fetchSize;
	}

	public void setFetchSize(int fetchSize)
	{
		this.fetchSize = fetchSize;
	}
	
	public boolean isTableFormat()
	{
		return tableFormat;
	}
	
	public void setTableFormat(boolean tableFormat)
	{
		this.tableFormat = tableFormat;
		PrintUtil.setTableFormat(tableFormat);
	}
	
	public boolean isPrintType()
	{
		return printType;
	}

	public void setPrintType(boolean printType)
	{
		this.printType = printType;
		SimplePrintUtil.setPrintType(printType);
	}

	public int getCollectionEntryPrintCount()
	{
		return collectionEntryPrintCount;
	}

	public void setCollectionEntryPrintCount(int collectionEntryPrintCount)
	{
		this.collectionEntryPrintCount = collectionEntryPrintCount;
		SimplePrintUtil.setCollectionEntryPrintCount(collectionEntryPrintCount);
	}

	public LookupService getLookupService()
	{
		return lookupService;
	}

	public CommandClient getCommandClient()
	{
		return commandClient;
	}

	public boolean isShowTime()
	{
		return showTime;
	}

	public void setShowTime(boolean showTime)
	{
		this.showTime = showTime;
	}

	public List getLsKeyList()
	{
		return lsKeyList;
	}

	public void setLsKeyList(List list)
	{
		this.lsKeyList = list;
	}
	
	public boolean isLocator()
	{
		return isLocator;
	}
	
	
	public String expandProperties(String value)
	{
		value = value.trim();
		
		// Find properties and place them in list.
		String split[] = value.split("\\$\\{");
		ArrayList<String> list = new ArrayList<String>();
		for (int i = 0; i < split.length; i++) {
			int index = split[i].indexOf('}');
			if (index != -1) {
				list.add(split[i].substring(0, index));
			}
		}
		
		// appply each property (key) in the list
		for (String key : list) {
			String val = getProperty(key);
			if (val == null) {
				value = value.replaceAll("\\$\\{" + key + "\\}", "");
			} else {
				value = value.replaceAll("\\$\\{" + key + "\\}", val);
			}
		}
		return value;
	}
	
	public String getProperty(String key)
	{
		return envProperties.getProperty(key);
	}
	
	public void setProperty(String key, String value)
	{
		if (value == null || value.length() == 0) {
			envProperties.remove(key);
		} else {
			envProperties.setProperty(key, value);
		}
	}
	
	public void printProperties()
	{
		ArrayList<String> list = new ArrayList(envProperties.keySet());
		Collections.sort(list);
		for (String key : list) {
			String value = getProperty(key);
			println(key + "=" + value);
		}
	}

	private void parseArgs(String args[])
	{
		String arg;
		
		for (int i = 0; i < args.length; i++) {
			arg = args[i];

			if (arg.equalsIgnoreCase("-?")) {
				usage();
			} else if (arg.equalsIgnoreCase("-c")) {
				i++;
				if (i < args.length) {
					dataSerializableClassNames = args[i];
				}
			} else if (arg.equalsIgnoreCase("-d")) {
				i++;
				if (i < args.length) {
					dataSerializablesFilePath = args[i];
				}
			} else if (arg.equalsIgnoreCase("-dir")) {
				i++;
				if (i < args.length) {
					jarDirectoryPath = args[i];
				}
			} else if (arg.equalsIgnoreCase("-i")) {
				i++;
				if (i < args.length) {
					inputFilePath = args[i];
				}
			} else if (arg.equalsIgnoreCase("-jar")) {
				i++;
				if (i < args.length) {
					jarPaths = args[i];
				}
			} else if (arg.equalsIgnoreCase("-l")) {
				i++;
				if (i < args.length) {
					locators = args[i];
				}
			} else if (arg.equalsIgnoreCase("-s")) {
				i++;
				if (i < args.length) {
					servers = args[i];
				}
			} else if (arg.equalsIgnoreCase("-advanced")) {
        isAdvancedMode = true;
      } else {
        System.out.println("Unknown option: '"+arg+"'");
        usage();
      }
		}
	}
	
	private static void usage()
	{
		String homeDir = System.getProperty("user.home");
		String fileSeparator = System.getProperty("file.separator");
		System.out.println();
		System.out.println("Usage:");
		System.out.println("   gfsh [-c <comma separated fully-qualified class names>]");
		System.out.println("        [-d <DataSerializables.txt file>]");
		System.out.println("        [-dir <directory>]");
		System.out.println("        [-i <.gfshrc file>]");
		System.out.println("        [-jar <jar paths>]");
		System.out.println("        [-l <host:port>|-s <host:port>]");
    System.out.println("        [-advanced]");
    System.out.println("        [-? | -h[elp]]");
		System.out.println();
		System.out.println("   -c <comma separated fully-qualified class names>] - specifies");
		System.out.println("        the names of classes to load. These classes typically");
		System.out.println("        contain static blocks that register GemFire data class");
		System.out.println("        ids via Instantiator.");
		System.out.println();
		System.out.println("   -d <DataSerializables.txt file>] - specifies the file path of");
		System.out.println("        DataSerializables.txt that overrides ");
		System.out.println("        " + homeDir + fileSeparator + ".gemfire" + fileSeparator + "etc" + fileSeparator + "DataSerializables.txt.");
		System.out.println("        This option is equivalent to 'class -d <DataSerializables.txt>.");
		System.out.println("        The file path can be relative or absolute.");
		System.out.println();
		System.out.println("   -dir <directory> - specifies the directory in which the jar files");
		System.out.println("        that contain data files are located. Gfsh loads all of the classes");
		System.out.println("        in the jar files including the jar files in the subdirectories.");
		System.out.println("        The directory can be relative or absolute. This options is");
		System.out.println("        equivalent to 'class -dir <directory>'.");
		System.out.println();
		System.out.println("   -i <.gfshrc path> - specifies the input file that overrides the default");
		System.out.println("        .gfshrc file in " + homeDir + ".");
		System.out.println("        The file path can be relative or absolute.");
		System.out.println();
		System.out.println("   -jar <jar paths> - specifies the jar paths separated by ',', ';',");
		System.out.println("        or ':'. Gfsh loads all the the classes in the jar files. The");
		System.out.println("        jar files can be relative or absolute. This options is equivalent");
		System.out.println("        to 'class -jar <jar paths>'.");
    System.out.println();
    System.out.println("   -advanced - enables these advanced commands : " + Arrays.toString(advancedCommands));
		System.out.println();
		System.out.println("   -l <host:port> - specifies locators.");
		System.out.println();
		System.out.println("   -s <host:port> - specifies cache servers.");
    System.out.println();
    System.out.println("   -version - shows gfsh version.");
    System.out.println();
    System.out.println("   -?  OR -help - displays this help message.");    
		System.out.println();
		System.out.println("Example 1: Start gfsh using a relative path");
		System.out.println("   cd /home/foo/app");
		System.out.println("   export GEMFIRE_APP_CLASSPATH=classes");
		System.out.println("   gfsh -l localhost:37000 -jar lib/foo.jar,lib/yong.jar");
		System.out.println();
		System.out.println("Example 2: Start gfsh using an app specific .gfshrc");
		System.out.println("   export GEMFIRE_APP_JAR_DIR=/home/foo/app/lib");
		System.out.println("   gfsh -l localhost:37000 -i /home/foo/app/.gfshrc");
		System.out.println();
		System.out.println("By default, during startup, gfsh sequentially executes the commands listed");
		System.out.println("in the .gfshrc file found in your home directory. The gfsh options");
		System.out.println("specified in the command line override the commands listed in .gfshrc.");
		System.out.println();
		System.exit(0);
	}

	public static void main(String args[]) throws Exception
	{
    if (args.length > 0) {
      if (args[0].equals("-h") || args[0].equals("-help") || args[0].equals("-?")) {
        usage();//System.exit is called from within usage()
      } else if (args[0].equals("version") || args[0].equals("-version")) {
        boolean fullVersion = (args.length > 1) && "FULL".equals(args[1].trim());
        System.out.println(GfshVersion.asString(fullVersion));
        return;
      }
    }

		Gfsh gfsh = new Gfsh(args);
		gfsh.go();
	}
}

/*
 * FIXME: We can simply add a loadXXX() method in this class and move the code
 * from static block to that method. This would not require us to load this 
 * class in Gfsh.initializeCache() 
 */
class DataSerializablesInitializer {

  private final static int POGO_CLASS_ID_BASE = Integer.getInteger("pogo.classId.base", 0).intValue();
  private final static int UTIL_CLASS_ID_BASE = Integer.getInteger("util.classId.base", POGO_CLASS_ID_BASE + 200).intValue();
  private final static int COMMAND_CLASS_ID_BASE = Integer.getInteger("command.classId.base", 300);
  
  //code from com/gemstone/gemfire/internal/tools/gfsh/app/command/DataSerializables.java
  static {

    // Allow the deprecated command.classId.base base if defined
    // else use util.cassId.base as the standard base. As of GemFire 6.5,
    // all add-on libraries are tied to a single base id, util.cassId.base.
    // POGO reserves class Ids less than 1000, i.e, 1-999, inclusive.

    String oldCommandBaseId = System.getProperty("command.classId.base");
    int classId;
    if (oldCommandBaseId != null) {
      classId = COMMAND_CLASS_ID_BASE;
    } else {
      classId = Integer.getInteger("util.classId.base", 200) + 100;
    }

    Instantiator.register(new Instantiator(CommandResults.class, classId++) {
      public DataSerializable newInstance() {
        return new CommandResults();
      }
    });

    Instantiator.register(new Instantiator(RegionCreateTask.class, classId++) {
      public DataSerializable newInstance() {
        return new RegionCreateTask();
      }
    });

    Instantiator.register(new Instantiator(RegionDestroyTask.class, classId++) {
      public DataSerializable newInstance() {
        return new RegionDestroyTask();
      }
    });

    Instantiator.register(new Instantiator(RegionPathTask.class, classId++) {
      public DataSerializable newInstance() {
        return new RegionPathTask();
      }
    });

    Instantiator.register(new Instantiator(ForceGCTask.class, classId++) {
      public DataSerializable newInstance() {
        return new ForceGCTask();
      }
    });

    Instantiator.register(new Instantiator(IndexInfoTask.class, classId++) {
      public DataSerializable newInstance() {
        return new IndexInfoTask();
      }
    });

    Instantiator.register(new Instantiator(QuerySizeTask.class, classId++) {
      public DataSerializable newInstance() {
        return new QuerySizeTask();
      }
    });

    Instantiator.register(new Instantiator(QueryTask.class, classId++) {
      public DataSerializable newInstance() {
        return new QueryTask();
      }
    });

    Instantiator.register(new Instantiator(IndexInfo.class, classId++) {
      public DataSerializable newInstance() {
        return new IndexInfo();
      }
    });

    Instantiator.register(new Instantiator(EntryMap.class, classId++) {
      public DataSerializable newInstance() {
        return new EntryMap();
      }
    });

    Instantiator.register(new Instantiator(
        PartitionedRegionAttributeTask.class, classId++) {
      public DataSerializable newInstance() {
        return new PartitionedRegionAttributeTask();
      }
    });

    Instantiator.register(new Instantiator(MemberInfo.class, classId++) {
      public DataSerializable newInstance() {
        return new MemberInfo();
      }
    });

    Instantiator.register(new Instantiator(RegionAttributeInfo.class, classId++) {
      public DataSerializable newInstance() {
        return new RegionAttributeInfo();
      }
    });

    Instantiator.register(new Instantiator(EchoTask.class, classId++) {
      public DataSerializable newInstance() {
        return new EchoTask();
      }
    });

    Instantiator.register(new Instantiator(RegionSizeTask.class, classId++) {
      public DataSerializable newInstance() {
        return new RegionSizeTask();
      }
    });

    Instantiator.register(new Instantiator(PartitionAttributeInfo.class,
        classId++) {
      public DataSerializable newInstance() {
        return new PartitionAttributeInfo();
      }
    });

    Instantiator.register(new Instantiator(RegionClearTask.class, classId++) {
      public DataSerializable newInstance() {
        return new RegionClearTask();
      }
    });
  } //static 1

  //code from com/gemstone/gemfire/internal/tools/gfsh/app/misc/util/DataSerializables.java 
  static {
    int classId = UTIL_CLASS_ID_BASE;

    // default: 200
    Instantiator.register(new Instantiator(GenericMessage.class, classId++) {
      public DataSerializable newInstance() {
        return new GenericMessage();
      }
    });

    // default: 201
    Instantiator.register(new Instantiator(ListMap.class, classId++) {
      public DataSerializable newInstance() {
        return new ListMap();
      }
    });

    // default: 202
    Instantiator.register(new Instantiator(ListMapMessage.class, classId++) {
      public DataSerializable newInstance() {
        return new ListMapMessage();
      }
    });

    // default: 203
    Instantiator.register(new Instantiator(ListMessage.class, classId++) {
      public DataSerializable newInstance() {
        return new ListMessage();
      }
    });

    // default: 204
    Instantiator.register(new Instantiator(MapMessage.class, classId++) {
      public DataSerializable newInstance() {
        return new MapMessage();
      }
    });

    // default: 205
    Instantiator.register(new Instantiator(MapLite.class, classId++) {
      public DataSerializable newInstance() {
        return new MapLite();
      }
    });
  } //static 2
  
}

