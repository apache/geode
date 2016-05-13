/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package javaclient;

import java.io.*;
import java.util.*;
import java.net.*;
import com.gemstone.gemfire.cache.util.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.*;
import java.util.concurrent.locks.*;
import com.gemstone.gemfire.cache.query.*;
import com.gemstone.gemfire.cache.query.types.*;
import java.io.*;

/**
 * This class is a command-line application that allows the user to
 * exercise GemFire's {@link com.gemstone.gemfire.cache cache
 * API}. The example allows the user to specify a <A
 * HREF="{@docRoot}/../cacheRunner/cache.xml">cache.xml</A>
 * file that specifies a parent region with certain properties and
 * then allows the user to exercise the cache API
 *
 * @author GemStone Systems, Inc.
 * @since 3.0
 */
public class CacheRunner {
	
	/** Cache <code>Region</code> currently reviewed by this example  */
	private Region currRegion;
	
	/** The cache used in the example */
	private Cache cache;
	
	/** This example's connection to the distributed system */
	private DistributedSystem system = null;
	
	/** This is used to store each Regions original attributes for reset purposes */
	private HashMap regionDefaultAttrMap = new HashMap();
	
	/** The cache.xml file used to declaratively configure the cache */
	private File xmlFile = null;
	
	/** output results? */
	private boolean echo = false;
	
	private String seqid = null;
	
	private String waittime = null;
	
	/** Maps the names of locked "items" (regions or entries) to their
	 * <code>Lock</code> */
	private HashMap lockedItemsMap = new HashMap();
	
	public static PrintStream out = System.out;
	
	/**
	 * Prints information on how this program should be used.
	 */
	void showHelp() {
		out.println();
		out.println("A distributed system is created with properties loaded from your ");
		out.println("gemfire.properties file.  You can specify alternative property files using");
		out.println("-DgemfirePropertyFile=path.");
		out.println();
		out.println("Entry creation and retrieval handles byte[] (default), String, Integer, ");
		out.println("and a complex object generated using gfgen.");
		out.println();
		out.println("You have to use mkrgn and chrgn to create and descend into a region");
		out.println("before using entry commands");
		out.println();
		out.println("Use a backward slash (\\) at the end of a line to continue a ");
		out.println("command on the next line. This is particularly useful for the");
		out.println("exec command.");
		out.println();
		out.println("Arguments can be wrapped in double-quotes so they can contain whitespace.");
		out.println();
		out.println("Commands:");
		out.println("ls");
		out.println("     List cache entries and subregions in current region and their stats.");
		out.println();
		out.println("lsAtts");
		out.println("     Lists the names of the region attributes templates stored in the cache.");
		out.println();
		out.println("status");
		out.println("     Display the current region global name, its stats, and the current");
		out.println("     attributes settings");
		out.println();
		out.println("create name [value=n [str|int|obj]] [keytype={int|str}]");
		out.println("     Define a new entry (see mkrgn to create a region). The complex object");
		out.println("     fields are filled based on the value given.");
		out.println();
		out.println("echo");
		out.println("     Toggle the echo setting. When echo is on then input commands are echoed to stdout.");
		out.println();
		out.println("put name {value=n [int|long|float|double|str|obj|usr]|valuesize=n} [keytype={int|str}]");
		out.println("     Associate a name with a value in the current region. As with create, the");
		out.println("     complex object fields are filled based on the value provided.");
		out.println();
		out.println("mput {keytype=[int|str] keyrange=n-m [base=\"AAA BBB\"] | k1 k2 .. kn} [value=n [valuetype=int|long|float|double|str|obj|usr] | valuesize=n] [seqid=x] [waittime=y]");
		out.println("     If valuetype is omited, it will be str. ");
		out.println("     If valuesize is specified, the valuetype should be str. Otherwise it will throw exception.");
		out.println();
		out.println("get name [keytype={int|str}]");
		out.println("     Get the value of a cache entry. ");
		out.println();
		out.println("mget keytype=[int|str] [base=\"AAA BBB\"] keyrange=n-m | k1 k2 .. kn [seqid=x] [waittime=y]");
		out.println("     Get values of multiple keys. ");
		out.println();
		out.println("run name number");
		out.println("     run a certain number of get() on this entry. ");
		out.println();
		out.println("reg {k1 k2 ... kn | keytype={int|str} [base=\"AAA BBB\"] keyrange=n-m | ALL_KEYS } [seqid=x]");
		out.println("     Use for client regions with servers set for notify-by-subscriptions. Registers interest in specific keys.");
		out.println();
		out.println("unreg {k1 k2 ... kn | keytype={int|str} [base=\"AAA BBB\"] keyrange=n-m | ALL_KEYS } [seqid=x]");
		out.println("     unregister the keys from interest list.");
		out.println();
		out.println("verify name {value=n [int|long|float|double|str|obj|usr] | valuesize=n} [keytype={int|str}] [seqid=x]");
		out.println("     verify the value specified is the same as the one get from cache.");
		out.println();
		out.println("mverify keytype={int|str} {keyrange=n-m [base=\"AAA BBB\"] | k1 k2 .. kn} {value [valuetype=int|long|float|double|str|obj|usr] | valuesize=n} [seqid=x]");
		out.println("     unregister the keys from interest list.");
		out.println();
		out.println("clear");
		out.println("     clear local region, i.e. local destroy all entries");
		out.println();
		out.println("des [-l] [name] [keytype={int|str}]");
		out.println("     Destroy an object or current region.  -l is for local destroy");
		out.println();
		out.println("mdes [-l] {k1 k2 ... kn | keytype={int|str} [base=\"AAA BBB\"] keyrange=n-m} [seqid=x]");
		out.println("     Destroy entries in current region.  -l is for local destroy");
		out.println();
		out.println("inv [-l] [name] [keytype={int|str}]");
		out.println("     Invalidate a cache entry or current region. -l is for local invalidation");
		out.println();
		out.println("minv [-l] {k1 k2 ... kn | keytype={int|str} [base=\"AAA BBB\"] keyrange=n-m} [seqid=x]");
		out.println("     Invalidate entries in current region.  -l is for local destroy");
		out.println();
		out.println("exec queryExpr");
		out.println("     Execute a query. All input after the exec command is considered the query string");
		out.println();
		out.println("mkidx name type idxExpr fromClause");
		out.println("     Create an query index.");
		out.println("        name         name of the index");
		out.println("        type         func | pkey");
		out.println("        idxExpr      the indexed expression (e.g. the attribute)");
		out.println("        fromExpr     the FROM clause");    
		out.println();    
		out.println("rmidx name");
		out.println("     Remove an query index.");
		out.println("        name         name of the index");
		out.println();
		out.println("indexes");
		out.println("     Prints information about all indexes in the cache. ");
		out.println();
		out.println("lock [name]");
		out.println("     Lock a cache entry or current region.");
		out.println();
		out.println("unlock [name]");
		out.println("     Unlock a cache entry or current region.");
		out.println();
		out.println("locks");
		out.println("     List the locks you hold");
		out.println();
		out.println("mkrgn name [attributeID]");
		out.println("     Create a subregion with the current attributes settings.");
		out.println("     The current attributes settings can be viewed with the status command");
		out.println("     and modified with the set and reset commands");
		out.println();
		out.println("reset");
		out.println("     Return the current attributes settings to the previously saved");
		out.println();
		out.println("save");
		out.println("     Save the current regions attributes");
		out.println();
		out.println("set [loader|listener|writer|capacitycontroller] [null]");
		out.println("     Add or remove a cache callback.  Accepted values: loader, listener,");
		out.println("     writer, and capacitycontroller. Use the optional null keyword to ");
		out.println("     remove the cache callback");
		out.println();
		out.println("set expiration <attribute> <value> <action>");
		out.println("     Set the value and action for an expiration attribute");
		out.println("       set expiration regionIdleTime 100 destroy");
		out.println("     The above example sets regionIdleTimeout to 100 ms and");
		out.println("     its action to destroy. Accepted attributes: regionIdleTime,");
		out.println("     entryIdleTime, regionTTL, entryTTL. Accepted actions: destroy,");
		out.println("     invalidate, localDestroy, localInvalidate");
		out.println();
		out.println("chrgn name");
		out.println("     Change current region (can use a local or global name)");
		out.println();
		out.println("chrgn ..");
		out.println("      Go up one region (parent region)");
		out.println();
		out.println("chrgn");
		out.println("     Go to cache root level");
		out.println();
		out.println("open");
		out.println("     Creates a Cache");
		out.println();
		out.println("load fileName");
		out.println("     Re-initializes the cache based using a cache.xml file");
		out.println();
		out.println("begin");
		out.println("     Begins a transaction in the current thread");
		out.println();
		out.println("commit");
		out.println("     Commits the current thread's transaction");
		out.println();
		out.println("rollback");
		out.println("     Rolls back the current thread's transaction");
		out.println();
		out.println("help or ?");
		out.println("     List command descriptions");
		out.println();
		out.println("exit or quit");
		out.println("     Closes the current cache and exits");
	}
	
	/**
	 * Initializes the <code>Cache</code> for this example program.
	 * Uses the {@link LoggingCacheListener}, {@link LoggingCacheLoader},
	 * {@link LoggingCacheWriter} }.
	 */
	void initialize() throws Exception {
		Properties props = new Properties();
		props.setProperty("name", "CacheRunner");
		if (this.xmlFile != null) {
			props.setProperty("cache-xml-file",this.xmlFile.toString());
		}
		system = DistributedSystem.connect(props);
		
		if (system != null) {
			cache = CacheFactory.create(system);     
			Iterator rIter = cache.rootRegions().iterator();
			if(rIter.hasNext()) {
				currRegion = (Region) rIter.next();
			}
			else {
				/* If no root region exists, create one with default attributes */
				out.println("No root region in cache. Creating a root, 'root'\nwith keys constrained to String for C cache access.\n");
				AttributesFactory fac = new AttributesFactory();
				fac.setKeyConstraint(String.class);
				currRegion = cache.createVMRegion("root", fac.createRegionAttributes());
			}
			
			AttributesMutator mutator = this.currRegion.getAttributesMutator();
			RegionAttributes currRegionAttributes = this.currRegion.getAttributes();
			if (currRegionAttributes.getCacheListener() == null) {
				// mutator.setCacheListener(new LoggingCacheListener());
			}
			
			CacheTransactionManager manager =
				cache.getCacheTransactionManager();
			manager.setListener(new LoggingTransactionListener());
			
			if (currRegionAttributes.getCacheWriter() == null) {
				mutator.setCacheWriter(new LoggingCacheWriter());
			}
			
			if (currRegionAttributes.getCacheLoader() == null) {
				mutator.setCacheLoader(new LoggingCacheLoader());
			}
			
			initRegionDefaultMap();
			
			cache.getLogger().info("Initialized");
		}
	}
	
	/** Populate a map that has all the orginal attributes for existing regions */
	private void initRegionDefaultMap() {
		Iterator rrIter = cache.rootRegions().iterator();
		Iterator rSubIter = null;
		Region cRegion = null;
		AttributesFactory fac = null;
		
		while(rrIter.hasNext()) {
			cRegion = (Region) rrIter.next();
			fac = new AttributesFactory(cRegion.getAttributes());
			regionDefaultAttrMap.put(cRegion.getFullPath(), fac.createRegionAttributes());
			rSubIter = cRegion.subregions(true).iterator();
			while(rSubIter.hasNext()) {
				cRegion = (Region) rSubIter.next();
				fac = new AttributesFactory(cRegion.getAttributes());
				regionDefaultAttrMap.put(cRegion.getFullPath(), fac.createRegionAttributes());
			}
		}
	}
	
	/**
	 * Sets whether commands from input should be echoed to output.
	 * Default is false.
	 */
	public void setEcho(boolean echo) {
		this.echo = echo;
	}
	
	/**
	 * Sets the <code>cache.xml</code> file used to declaratively
	 * initialize the cache in this example.
	 */
	public void setXmlFile(File xmlFile) {
		this.xmlFile = xmlFile;
	}
	
	/**
	 * Parses the command line and runs the <code>CacheRunner</code>
	 * example.
	 */
	public static void main(String[] args) throws Exception {
		if (args.length == 0) {
			out.println("Usage: java CacheRunner <cache.xml> [-batch [file=<cmdfile> | \"command\"]] [logfile=xxx]");
			// out.println("To use shared memory: java -Dgemfire.enable-shared-memory=true <cache.xml>");
			System.exit(1);
		}
		String xmlFileName = args[0];   
		File xmlFile = new File(xmlFileName);
		if (!xmlFile.exists()) {
			out.println("Supplied Cache config file <cache.xml> does not exist");
			System.exit(1);
		}
		
		// specify my own log file, otherwise use System.out
		for (int i=1; i<args.length; i++) {
			if (args[i].startsWith("logfile=")) {
				String log_file = new String(args[i].substring(8));
				FileOutputStream log = new FileOutputStream(log_file, true);
				out = new PrintStream(log);
				break;
			}
		}
		
		boolean batch_mode = false;
		String command;
		if (args.length > 1 && args[1].equals("-batch")) {
			batch_mode = true;
			if (args.length != 3 && args.length != 4) {
				out.println("Usage: java CacheRunner <cache.xml> [-batch [file=<cmdfile> | \"command\"] [logfile=xxx]");
				System.exit(1);
			}
		}
		
		User u = new User("", 0);
		ExampleObject o = new ExampleObject();
		
		CacheRunner runner = new CacheRunner();
		runner.setXmlFile(xmlFile);
		runner.initialize();
		
		if (!batch_mode) {
			runner.go(null);
		} else {
			// do batch commands
			command = args[2];
			if (command.startsWith("file=")) {
				String cmd_file = command.substring(5);
				runner.go(cmd_file);
			}
			else {
				runner.execCommand(command);
			}
		}
		System.exit(0);
	}

	void execCommand(String command)
	{
		try {
			if (command == null /* EOF */ ||
					command.startsWith("exit") || command.startsWith("quit")) {
				this.cache.close();
				this.system.disconnect();
				Thread.sleep(5000);
				long ts2 = System.currentTimeMillis();
				Date date = new Date(ts2);
				out.println("Java client exits at "+ts2+" Date: "+date.toString());
				System.exit(0);
//				return;
			}
			else if (command.startsWith("#")) {
				return;
			}
			else if (command.startsWith("echo")) {
				this.echo = !this.echo;
				out.println("echo is " + (this.echo ? "on." : "off."));
			}
			else if (command.startsWith("set")) {
				setRgnAttr(command);
			}
			else if (command.startsWith("run")) {
				run(command);
			}
			else if (command.startsWith("reg")) {
				registerInterest(command);
			}
			else if (command.startsWith("unreg")) {
				unregisterInterest(command);
			}
			else if (command.startsWith("clear")) {
				clear();
			}
			else if (command.startsWith("put")) {
				put(command);
			}
			else if (command.startsWith("mput")) {
				mput(command);
			}
			else if (command.startsWith("cre")) {
				create(command);
			}
			else if (command.startsWith("get")) {
				get(command);
			}
			else if (command.startsWith("mget")) {
				mget(command);
			}
			else if (command.startsWith("verify")) {
				verify(command);
			}
			else if (command.startsWith("mverify")) {
				mverify(command);
			}
			else if (command.startsWith("reset")) {
				reset();
			}
			else if (command.startsWith("inv")) {
				inv(command);
			}
			else if (command.startsWith("minv")) {
				minv(command);
			}
			else if (command.startsWith("des")) {
				des(command);
			}
			else if (command.startsWith("mdes")) {
				mdes(command);
			}
			else if (command.startsWith("lsAtts")) {
				lsAtts(command);
			}
			else if (command.startsWith("ls")) {
				ls(command);
			}
			else if (command.startsWith("stat")) {
				status(command);
			}
			else if (command.startsWith("mkr")) {
				mkrgn(command);
			}
			else if (command.startsWith("chr")) {
				chrgn(command);
			}
			else if (command.startsWith("open")) {
				open(command);
			}
			else if (command.startsWith("load")) {
				load(command);
			}
			else if (command.startsWith("begin")) {
				begin(command);
			}
			else if (command.startsWith("commit")) {
				commit(command);
			}
			else if (command.startsWith("rollback")) {
				rollback(command);
			}
			else if (command.startsWith("locks")) {
				showlocks();
			}
			else if (command.startsWith("lock")) {
				lock(command);
			}
			else if (command.startsWith("unlock")) {
				unlock(command);
			}
			else if (command.startsWith("save")) {
				save();
			}
			else if (command.startsWith("help") || command.startsWith("?")){
				showHelp();
			}
			else if (command.startsWith("exec")) {
				exec(command);
			}
			// case insensitive command
			else if (command.toLowerCase().startsWith("mkidx")) {
				mkidx(command);
			}
			else if (command.toLowerCase().startsWith("rmidx")) {
				rmidx(command);
			}
			else if (command.toLowerCase().startsWith("indexes")) {
				indexes(command);
			}
			else if (command.length() != 0) {
				out.println("Unrecognized command. Enter 'help' or '?' to get a list of commands.");
			}
		} catch (CacheException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			long ts2 = System.currentTimeMillis();
			Date date = new Date(ts2);
			out.println("End Time: "+ts2+" Date: "+date.toString());
			if (seqid != null) {
				out.println("mget seqid "+seqid+" finished (failed at exception).");
				seqid = null;
			}
		} catch (QueryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			long ts2 = System.currentTimeMillis();
			Date date = new Date(ts2);
			out.println("End Time: "+ts2+" Date: "+date.toString());
			if (seqid != null) {
				out.println("mget seqid "+seqid+" finished (failed at exception).");
				seqid = null;
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			long ts2 = System.currentTimeMillis();
			Date date = new Date(ts2);
			out.println("End Time: "+ts2+" Date: "+date.toString());
			if (seqid != null) {
				out.println("mget seqid "+seqid+" finished (failed at exception).");
				seqid = null;
			}
		}
	}
	/**
	 * Prompts the user for input and executes the command accordingly.
	 */
	void go(String inputFile) {
		out.println("Enter 'help' or '?' for help at the command prompt.\n");
		BufferedReader bin = null;
		if (inputFile == null || inputFile.equals("")) {
			bin = new BufferedReader(new InputStreamReader(System.in));
		}
		else {
			try {
				bin = new BufferedReader(new FileReader(inputFile));
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		while(true) {
			try {
				String command = getLine(bin);
				execCommand(command);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}     
		}
	}
	
	private String getLine(BufferedReader bin) throws IOException {  
		String prompt = this.currRegion.getFullPath();
		TransactionId xid =
			this.currRegion.getCache().getCacheTransactionManager().getTransactionId();
		if (xid != null) {
			prompt += " (TXN " + xid + ")";
		}
		
		out.print(prompt + "> ");
		out.flush();
		
		StringBuffer cmdBuffer = null;
		boolean keepGoing;
		int timeoutcnt = 0;
		do {
			keepGoing = false;
			String nextLine = bin.readLine();
			
			// if nextLine is null then we encountered EOF.
			// In that case, leave cmdBuffer null if it is still null
			
//			if (this.echo) {
//			out.println(nextLine == null ? "EOF" : nextLine);
//			}
			
			if (nextLine == null) {
				try {
					Thread.sleep(10);
					timeoutcnt += 10;
					if (timeoutcnt > 7200000) {
						// default timeout is 2 hours
						keepGoing = false;
					} else {
						keepGoing = true;
					}
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				continue;
				// break;
			}
			else if (cmdBuffer == null) {
				cmdBuffer = new StringBuffer();
			}
			
			// if last character is a backward slash, replace backward slash with
			// LF and continue to next line
			if (nextLine.endsWith("\\")) {
				nextLine = nextLine.substring(0, nextLine.length() - 1);
				keepGoing = true;
			}
			cmdBuffer.append(nextLine);
			if (keepGoing) {
				cmdBuffer.append('\n');
			}
		} while (keepGoing);
		
		// if (this.echo) out.println(cmdBuffer);
		out.println(cmdBuffer);
		
		return cmdBuffer == null ? null : cmdBuffer.toString();    
	}
	
	// ************ Command implementation methods ****************************
	
	/**
	 * Executes a Query.
	 * @see Query
	 */
	void exec(String command) throws QueryException {
		// remove the "exec" command from the string
		String execCmd = new StringTokenizer(command).nextToken();
		String queryString = command.substring(execCmd.length());
		
		try {      
			Query query = this.cache.getQueryService().newQuery(queryString);
			long startNanos = System.nanoTime();
			Object results = query.execute();
			long execNanos = System.nanoTime() - startNanos; 
			out.println("Query Executed in " + (execNanos / 1e6) + " ms.");
			
			if (results instanceof SelectResults) {
				StringBuffer sb = new StringBuffer();
				
				SelectResults sr = (SelectResults) results;
				CollectionType type = sr.getCollectionType();
				sb.append(sr.size());
				sb.append(" results in a collection of type ");
				sb.append(type);
				sb.append("\n");
				
				sb.append("  [");
				if (sr.isModifiable()) {
					sb.append("modifiable, ");
				} else {
					sb.append("unmodifiable, ");
				}
				if (type.isOrdered()) {
					sb.append("ordered, ");
				} else {
					sb.append("unordered, ");
				}
				if (type.allowsDuplicates()) {
					sb.append("duplicates");
				} else {
					sb.append("no duplicates");
				}
				sb.append("]\n");
				
				ObjectType elementType = type.getElementType();
				for (Iterator iter = sr.iterator(); iter.hasNext(); ) {
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
							sb.append(fieldType);
							sb.append(" ");
							sb.append(fieldName);
							sb.append(" = ");
							sb.append(fieldValue);
							sb.append("\n");
						}
						
					} else {
						sb.append("  ");
						sb.append(element);
					}
					
					sb.append("\n");
				}
				
				out.println(sb);
				
			} else {
				// Just a regular object
				out.println("results are not SelectResults");
				out.println(results);
			}
		}
		catch (UnsupportedOperationException e) {
			// print a nicer message than a stack trace
			out.println("Error: Unsupported Feature: " + e.getMessage());
		}
	}
	
	/**
	 * Creates an index.
	 * Arguments are type, name, idxExpr, fromClause
	 */
	void mkidx(String command) throws QueryException {
		List args = new ArrayList();
		parseCommand(command, args);
		if (args.size() < 5) {
			out.println("mkidx requires 4 args: name type idxExpr fromClause");
			return;
		}
		String name = (String)args.get(1);
		String type = (String)args.get(2);
		String idxExpr = (String)args.get(3);
		String fromClause = (String)args.get(4);
		
		IndexType indexType;
		if (type.toLowerCase().startsWith("func")) {
			indexType = IndexType.FUNCTIONAL;
			
		} else if (type.toLowerCase().startsWith("pkey")) {
			indexType = IndexType.PRIMARY_KEY;
			
		} else {
			throw new UnsupportedOperationException("Currently only support " +
					"functional and primary key indexes. No support for " + type + ".");
		}
		
		QueryService qs = this.cache.getQueryService();
		qs.createIndex(name, indexType, idxExpr, fromClause);
	}
	
	/**
	 * Removes an index.
	 * Argument is type
	 */
	void rmidx(String command) throws QueryException {
		List args = new ArrayList();
		parseCommand(command, args);
		if (args.size() < 1) {
			out.println("rmidx requires 1 arg: name");
			return;
		}
		String name = (String)args.get(1);
		
		QueryService qs = this.cache.getQueryService();
		Index index = qs.getIndex(this.currRegion, name);
		if (index == null) {
			String s = "There is no index named \"" + name +
			"\" in region " + this.currRegion.getFullPath();
			out.println(s);
			
		} else {
			qs.removeIndex(index);
		}
	}
	
	/**
	 * Prints out information about all of the indexes built in the
	 * cache. 
	 */
	void indexes(String command) {
		QueryService qs = this.cache.getQueryService();
		Collection indexes = qs.getIndexes();
		StringBuffer sb = new StringBuffer();
		sb.append("There are ");
		sb.append(indexes.size());
		sb.append(" indexes in this cache\n");
		
		for (Iterator iter = indexes.iterator(); iter.hasNext(); ) {
			Index index = (Index) iter.next();
			sb.append("  ");
			sb.append(index.getType());
			sb.append(" index \"");
			sb.append(index.getName());
			sb.append(" on region ");
			sb.append(index.getRegion().getFullPath());
			sb.append("\n");
			
			String expr = index.getIndexedExpression();
			if (expr != null) {
				sb.append("    Indexed expression: ");
				sb.append(expr);
				sb.append("\n");
			}
			
			String from = index.getFromClause();
			if (from != null) {
				sb.append("    From: ");
				sb.append(from);
				sb.append("\n");
			}
			
			String projection = index.getProjectionAttributes();
			if (projection != null) {
				sb.append("    Projection: ");
				sb.append(projection);
				sb.append("\n");
			}
			
			sb.append("    Statistics\n");
			IndexStatistics stats = index.getStatistics();
			sb.append("      ");
			sb.append(stats.getTotalUses());
			sb.append(" uses\n");
			
			sb.append("      ");
			sb.append(stats.getNumberOfKeys());
			sb.append(" keys, ");
			sb.append(stats.getNumberOfValues());
			sb.append(" values\n");
			
			sb.append("      ");
			sb.append(stats.getNumUpdates());
			sb.append(" updates totaling ");
			sb.append(stats.getTotalUpdateTime() / 1e6);
			sb.append(" ms.\n");
		}
		out.println(sb.toString());
	}
	
	/**
	 * Prints out information about the current region
	 *
	 * @see Region#getStatistics
	 */
	void status(String command) throws CacheException {
		out.println("Current Region:\n\t" + this.currRegion.getFullPath());
		RegionAttributes attrs = this.currRegion.getAttributes();
		if (attrs.getStatisticsEnabled()) {
			CacheStatistics stats = this.currRegion.getStatistics();
			out.println("Stats:\n\tHitCount is " + stats.getHitCount() +
					"\n\tMissCount is " + stats.getMissCount() +
					"\n\tLastAccessedTime is " + stats.getLastAccessedTime() +
					"\n\tLastModifiedTime is " + stats.getLastModifiedTime());
		}
		out.println("Region CacheCallbacks:" +
				"\n\tCacheListener: " + attrs.getCacheListener() + 
				"\n\tCacheLoader: " + attrs.getCacheLoader() + 
				"\n\tCacheWriter: " + attrs.getCacheWriter() + 
				"\nRegion Expiration Settings:" + 
				"\n\tRegionIdleTimeOut: " + attrs.getRegionIdleTimeout() +
				"\n\tEntryIdleTimeout: " +  attrs.getEntryIdleTimeout() + 
				"\n\tEntryTimeToLive: " + attrs.getEntryTimeToLive() + 
				"\n\tRegionTimeToLive: " +  attrs.getRegionTimeToLive());
	}
	
	/**
	 * Creates a new subregion of the current region
	 *
	 * @see Region#createSubregion
	 */
	void mkrgn(String command) throws CacheException {
		String name = parseName(command);
		if (name != null) {
			RegionAttributes attrs = this.currRegion.getAttributes();
			AttributesFactory fac = new AttributesFactory(attrs);
			
			attrs = fac.createRegionAttributes();
			Region nr = this.currRegion.createSubregion(name, attrs);
			regionDefaultAttrMap.put(nr.getFullPath(), fac.createRegionAttributes());
		}
	}
	
	/**
	 * Prints out information about the region entries that are
	 * currently locked.
	 */
	void showlocks() {
		Iterator iterator = lockedItemsMap.keySet().iterator();
		while(iterator.hasNext()) {
			String name = (String) iterator.next();
			out.println("Locked object name " + name);
		}
	}
	
	/**
	 * Locks the current region or an entry in the current region based
	 * on the given <code>command</code>.
	 *
	 * @see Region#getRegionDistributedLock
	 * @see Region#getDistributedLock
	 */
	void lock(String command) {
		if (command.indexOf(' ') < 0) {
			//Lock the entire region
			String serviceName = this.currRegion.getFullPath();
			if (!lockedItemsMap.containsKey(serviceName)) {
				Lock lock = this.currRegion.getRegionDistributedLock();
				lock.lock();
				lockedItemsMap.put(serviceName, lock);
			}
			
		}
		else {
			String name = parseName(command);
			if (name != null) {
				if (!lockedItemsMap.containsKey(name)) {
					Lock lock = this.currRegion.getDistributedLock(name);
					lock.lock();
					lockedItemsMap.put(name, lock);
				}
			}
		}
	}
	
	/**
	 * Unlocks the current region or an entry in the current region
	 * based on the given <code>command</code>.
	 *
	 * @see Lock#unlock
	 */
	void unlock(String command) {
		if (command.indexOf(' ') < 0) {
			if (lockedItemsMap.containsKey(this.currRegion.getFullPath())) {
				Lock lock = (Lock) lockedItemsMap.remove(this.currRegion.getFullPath());
				lock.unlock();
			}
			else {
				out.println(" This region is not currently locked");
			}
			
		}
		else {
			String name = parseName(command);
			if ((name != null) && (lockedItemsMap.containsKey(name))) {
				Lock lock = (Lock) lockedItemsMap.remove(name);
				lock.unlock();
			}
			else {
				out.println(" This entry is not currently locked");
			}
			
		}
		
	}
	
	/**
	 * Changes the current region to another as specified by
	 * <code>command</code>.
	 *
	 * @see Cache#getRegion
	 * @see Region#getSubregion
	 * @see Region#getParentRegion
	 */
	void chrgn(String command) throws CacheException {
		LinkedList list = new LinkedList();
		parseCommand(command, list);
		if (list.size() == 1) {
			Iterator rIter = cache.rootRegions().iterator();
			if(rIter.hasNext()) {
				this.currRegion = (Region) rIter.next();
			}
			return;
		}
		
		String name = (String) list.get(1);
		Region tmpRgn;
		if (name.equals("..")) {
			tmpRgn = this.currRegion.getParentRegion();
		}
		else {
			tmpRgn = this.currRegion.getSubregion(name);
		}
		if (tmpRgn != null)
			this.currRegion = tmpRgn;
		else
			out.println("Region " + name + " not found");
	}
	
	/**
	 * Invalidates (either local or distributed) a region or region
	 * entry depending on the contents of <code>command</code>.
	 *
	 * @see Region#invalidateRegion
	 * @see Region#invalidate
	 */
	void inv(String command) throws CacheException {
		LinkedList list = new LinkedList();
		parseCommand(command,list);
		String arg1 = null;
		String arg2 = null;
		boolean inv_l = false;
		String keytype = getKeyType(list);
		switch (list.size()) {
		case 1:
			// inv followed by nothing invalidates the current region
			this.currRegion.invalidateRegion();
			break;
		case 2:
			arg1 = (String) list.get(1);
			inv_l = arg1.equals("-l");
			if (inv_l) {
				// inv -l local invalidates current region
				this.currRegion.localInvalidateRegion();
				
			}
			else {
				// inv name invalidate the entry name in current region
				Object keyO = arg1;
				if (keytype.equals("int")) {
					keyO = Integer.valueOf(arg1);
				}
				this.currRegion.invalidate(keyO);
			}
			break;
		case 3:
			// inv -l name local invalidates name in current region
			arg1 = (String) list.get(1);
			arg2 = (String) list.get(2);
			inv_l = arg1.equals("-l");
			if (inv_l) {
				Object keyO = arg2;
				if (keytype.equals("int")) {
					keyO = Integer.valueOf(arg2);
				}
				this.currRegion.localInvalidate(keyO);
			}
			break;
		default:
			break;
		}
		
		
	}
	
	/**
	 * Reset the pending region attributes to the current region
	 */
	void reset() throws CacheException {
		RegionAttributes defAttrs = (RegionAttributes) 
		regionDefaultAttrMap.get(this.currRegion.getFullPath());
		if (defAttrs != null) {
			AttributesMutator mutator = this.currRegion.getAttributesMutator();
			mutator.setCacheListener(defAttrs.getCacheListener());
			mutator.setCacheLoader(defAttrs.getCacheLoader());
			mutator.setCacheWriter(defAttrs.getCacheWriter());
			mutator.setRegionIdleTimeout(defAttrs.getRegionIdleTimeout());
			mutator.setEntryIdleTimeout(defAttrs.getEntryIdleTimeout());
			mutator.setEntryTimeToLive(defAttrs.getEntryTimeToLive());
			mutator.setRegionTimeToLive(defAttrs.getRegionTimeToLive());
			out.println("The attributes for Region " + 
					this.currRegion.getFullPath() + 
			" have been reset to the previously saved settings");
		}
	}
	
	/**
	 * Save the region attributes of the current region
	 */
	void save() throws CacheException {
		AttributesFactory fac = 
			new AttributesFactory(this.currRegion.getAttributes());
		regionDefaultAttrMap.put(this.currRegion.getFullPath(), 
				fac.createRegionAttributes());
		out.println("The attributes for Region " + 
				this.currRegion.getFullPath() + 
		" have been saved.");
	}
	
	/**
	 * Prints out the current region attributes
	 *
	 * @see Region#getAttributes
	 */
	void attr(String command) throws CacheException {
		RegionAttributes regionAttr = this.currRegion.getAttributes();
		out.println("region attributes: " + regionAttr.toString());
	}
	
	/**
	 * Gets a cached object from the current region and prints out its
	 * <code>String</code> value.
	 *
	 * @see Region#get
	 */
	void get(String command) throws CacheException {
		LinkedList list = new LinkedList();
		parseCommand(command, list);
		if (list.size() ==1) {
			out.println("Usage:get name [keytype={int|str}]");
			return;
		}
		String name = (String) list.get(1);
		String keytype = getKeyType(list);
		Object keyO = name;
		if (keytype.equals("int")) {
			Integer key = Integer.valueOf(name);
			keyO = key;
		}
		Object valueBytes = this.currRegion.get(keyO);
		printEntry(keyO, valueBytes);
	}
	
	/**
	 * clear local region
	 *
	 */
	void clear() throws CacheException {
		Iterator it = this.currRegion.keys().iterator();
		while (it.hasNext()) {
			Object key = it.next();
			if (key instanceof Integer) {
				Integer entryName = (Integer)key;
				this.currRegion.localDestroy(entryName);
                        } else {
				String entryName = (String)key;
				this.currRegion.localDestroy(entryName);
			}
		}	  
	}
	
	void run(String command) throws CacheException {
		LinkedList list = new LinkedList();
		parseCommand(command, list);
		if (list.size() <3) {
			out.println("Usage:run numberOfOp sizeOfData");
		}
		String num_str = (String) list.get(1);
		String size_str = (String) list.get(2);
		
		int number = 100;
		if (num_str != null) {
			number = Integer.parseInt(num_str);
		}
		
		int size = 100;
		if (size_str != null) {
			size = Integer.parseInt(size_str);
		}
		byte[] buf = new byte[size];
		for (int i=0;i<size;i++) {
			buf[i] = 'A';
		}
		String value = new String(buf);
		Vector keys = new Vector();
		keys.clear();
		for (int i=0; i<number; i++) {
			keys.add(new Integer(i));
		} 
		
//		for (int j=0; j<100; j++) {
		{
			long startTime = System.currentTimeMillis();
			try {
				for (int i=0; i<number; i++) {
					this.currRegion.put(keys.elementAt(i),value);
				}
			} catch (Exception e) {
				System.exit(1);
			}
			long finishTime = System.currentTimeMillis();
			long sec = (finishTime - startTime)/1000;
			long usec = (finishTime - startTime)%1000;
			float rst = (float)number/(finishTime - startTime)*1000;
			out.println("Number of put(): "+number+" Time consumed: "+sec+"."+usec+" Ops/second:"+rst);
		}
		
		try {
			for (int i=0; i<number; i++) {
				// this.currRegion.invalidate(keys.elementAt(i));
				this.currRegion.localDestroy(keys.elementAt(i));
			}
		} catch (Exception e) {
			System.exit(1);
		}
		try {
			Thread.sleep(10000);
		} catch (Exception e) {}
		
		{
			long startTime = System.currentTimeMillis();
			try {
				Object valueBytes = null;
				for (int i=0; i<number; i++) {
					valueBytes = this.currRegion.get(keys.elementAt(i));
					// Thread.sleep(1000);
				}
				// printEntry(keys.elementAt(0).toString(), valueBytes);
			} catch (Exception e) {
				System.exit(1);
			}
			long finishTime = System.currentTimeMillis();
			long sec = (finishTime - startTime)/1000;
			long usec = (finishTime - startTime)%1000;
			float rst = (float)number/(finishTime - startTime)*1000;
			out.println("Number of get(): "+number+" Time consumed: "+sec+"."+usec+" Ops/second:"+rst);
		}
		
		
		{
			long startTime = System.currentTimeMillis();
			try {
				for (int i=0; i<number; i++) {
					this.currRegion.destroy(keys.elementAt(i));
					// Thread.sleep(1000);
				}
			} catch (Exception e) {
				System.exit(1);
			}
			long finishTime = System.currentTimeMillis();
			long sec = (finishTime - startTime)/1000;
			long usec = (finishTime - startTime)%1000;
			float rst = (float)number/(finishTime - startTime)*1000;
			out.println("Number of destroy(): "+number+" Time consumed: "+sec+"."+usec+" Ops/second:"+rst);
		}
		
		{
			long startTime = System.currentTimeMillis();
			try {
				Object valueBytes = null;
				for (int i=0; i<number; i++) {
					valueBytes = this.currRegion.get(keys.elementAt(i));
				}
				printEntry(keys.elementAt(0).toString(), valueBytes);
			} catch (Exception e) {
				System.exit(1);
			}
			long finishTime = System.currentTimeMillis();
			long sec = (finishTime - startTime)/1000;
			long usec = (finishTime - startTime)%1000;
			float rst = (float)number/(finishTime - startTime)*1000;
			out.println("Number of get(): "+number+" Time consumed: "+sec+"."+usec+" Ops/second:"+rst);
		}
//		}
	}
	
	/**
	 * Registers interst with the server in specific keys.
	 * This is only applicable to client regions and the call throws
	 * an exception if there is no BridgeWriter installed. We just
	 * let that pop up for the user of this app so they can see what
	 * their program will get.
	 *
	 * @see Region#registerInterest
	 */
	void registerInterest(String command) throws CacheException {
		LinkedList keys = new LinkedList();
		LinkedList values = new LinkedList();
		boolean status = getKeyValueList(command, keys, values);
		if (status == false) {
			out.println("Usage:reg {k1 k2 ... kn | keytype={int|str} [base=\"AAA BBB\"] keyrange=n-m | ALL_KEYS }");
			return;
		}
		long ts1 = System.currentTimeMillis();
		Date date = new Date(ts1);
		out.println("Start Time: "+ts1+" Date: "+date.toString());
		Object o = keys.get(0);
		if (o instanceof String && ((String)o).equals("ALL_KEYS")) {
			this.currRegion.registerInterest("ALL_KEYS");
		} else {
			this.currRegion.registerInterest(keys, InterestResultPolicy.NONE);			
		}
		long ts2 = System.currentTimeMillis();
		long diff = ts2 - ts1;
		if (diff == 0) diff = 1;
		date = new Date(ts2);
		out.println("End Time: "+ts2+" Date: "+date.toString());
		if (seqid != null) {
			out.println("reg seqid "+seqid+" finished. Consumed: "+diff+" ms. "+keys.size()+" ops, "+keys.size()*1000/diff+ " ops/sec.");
			seqid = null;
		}
	}
	
	/**
	 * Unregisters interst with the server in specific keys.
	 * This is only applicable to client regions and the call throws
	 * an exception if there is no BridgeWriter installed. We just
	 * let that pop up for the user of this app so they can see what
	 * their program will get.
	 *
	 * @see Region#registerInterest
	 */
	void unregisterInterest(String command) throws CacheException {
		LinkedList keys = new LinkedList();
		LinkedList values = new LinkedList();
		boolean status = getKeyValueList(command, keys, values);
		if (status == false) {
			out.println("Usage:unreg {k1 k2 ... kn | keytype={int|str} [base=\"AAA BBB\"] keyrange=n-m | ALL_KEYS }");
			return;
		}

		long ts1 = System.currentTimeMillis();
		Date date = new Date(ts1);
		out.println("Start Time: "+ts1+" Date: "+date.toString());
		Object o = keys.get(0);
		if (o instanceof String && ((String)o).equals("ALL_KEYS")) {
			this.currRegion.unregisterInterest("ALL_KEYS");
		} else {
			this.currRegion.unregisterInterest(keys);
		}
		long ts2 = System.currentTimeMillis();
		long diff = ts2 - ts1;
		if (diff == 0) diff = 1;
		date = new Date(ts2);
		out.println("End Time: "+ts2+" Date: "+date.toString());
		if (seqid != null) {
			out.println("unreg seqid "+seqid+" finished. Consumed: "+diff+" ms. "+keys.size()+" ops, "+keys.size()*1000/diff+ " ops/sec.");
			seqid = null;
		}
	}
	
	private Object createValue(String objectType, String value) {
		if (objectType.equalsIgnoreCase("int")) {
			return Integer.valueOf(value);
		}
		else if (objectType.equalsIgnoreCase("long")) {
			return Long.valueOf(value);
		}
		else if (objectType.equalsIgnoreCase("float")) {
			return Float.valueOf(value);
		}
		else if (objectType.equalsIgnoreCase("double")) {
			return Double.valueOf(value);
		}
		else if (objectType.equalsIgnoreCase("str")) {
			return value;
		}
		else if (objectType.equalsIgnoreCase("obj")) {
			ExampleObject newObj = new ExampleObject(value);
//			try { 
//				newObj.setDouble_field(Double.valueOf(value).doubleValue()) ;
//				newObj.setFloat_field(Float.valueOf(value).floatValue()) ;
//				newObj.setLong_field(Long.parseLong(value)) ;
//				newObj.setInt_field(Integer.parseInt(value)) ;
//				newObj.setShort_field(Short.parseShort(value)) ;
//			} catch (Exception e)  {
//				e.printStackTrace();
//			}
//			newObj.setString_field(value) ;
//			Vector v = new Vector();
//			for (int i=0; i<3; i++) {
//				v.addElement(value);
//			}
//			newObj.setString_vector(v);
			return newObj;
		}
		else if (objectType.equalsIgnoreCase("usr")) {
			try {
				int userId = Integer.parseInt(value);
				String userName = value;
				User newUsr = new User(userName, userId);
//				ExampleObject newObj = new ExampleObject();
//				newObj.setDouble_field(Double.valueOf(value).doubleValue()) ;
//				newObj.setFloat_field(Float.valueOf(value).floatValue()) ;
//				newObj.setLong_field(Long.parseLong(value)) ;
//				newObj.setInt_field(Integer.parseInt(value)) ;
//				newObj.setShort_field(Short.parseShort(value)) ;
//				newObj.setString_field(value) ;
//				Vector v = new Vector();
//				for (int i=0; i<3; i++) {
//					v.addElement(value);
//				}
//				newObj.setString_vector(v);
//				newUsr.setEO(newObj);
				return newUsr;
			} catch (NumberFormatException e) {
				e.printStackTrace();
			}
		}
		else if (objectType.equalsIgnoreCase("bytes")) {
			int size = Integer.parseInt(value);
			byte[] byte_array = new byte[size];
			for (int i=0; i<size; i++) {
				byte_array[i] = 'A';
			}
			return byte_array;
		}
		else {
			out.println("Invalid object type specified. Please see help.");
			return null;
		}
		return null;
	}
	
	/**
	 * Creates a new entry in the current region
	 *
	 * @see Region#create
	 */
	void create(String command) throws CacheException {
		LinkedList list = new LinkedList();
		parseCommand(command,list);
		if (list.size() < 2 ) {
			out.println("Usage: create name [value=n [str|int|obj]] [keytype={int|str}]");
		}
		else {
			String name = (String) list.get(1);
			String keytype = getKeyType(list);
			Object keyO = null;
			if (keytype.equals("int")) {
				Integer key = Integer.valueOf(name);
				keyO = key;
			} else {
				keyO = name;
			}
			
			if (list.size() > 2) {
				String value = (String) list.get(2);
				if (list.size() > 3) {
					String objectType = (String) list.get(3);
					this.currRegion.create(keyO,createValue(objectType, value));
				}
				else {
					this.currRegion.create(keyO,value.getBytes());
				}
			}
			else {
				this.currRegion.create(keyO,(String)null);
			}
		}
	}
	
	/**
	 * Put an entry into the current region
	 *
	 * @see Region#put
	 */
	void put(String command) throws CacheException {
		LinkedList list = new LinkedList();
		parseCommand(command, list);
		if (list.size() < 3 ) {
			out.println("Usage: put name {value=n [int|long|flaot|double|str|obj|usr]|valuesize=n} [keytype={int|str}]");
		}
		else {
			String name = (String) list.get(1);
			Object keyO = name;
			String keytype = getKeyType(list);
			if (keytype.equals("int")) {
				Integer key = Integer.valueOf(name);
				keyO = key;
			}
			String s = (String) list.get(2);
			
			if (s.startsWith("valuesize=")) {
				String valuesize = s.substring(10);
				this.currRegion.put(keyO,createValue("bytes", valuesize));
			} else if (s.startsWith("value=")){
				String value = s.substring(6);
				String objectType = null;
				if (list.size() > 3) objectType = (String)list.get(3);
				if (objectType == null) objectType = "str";
				this.currRegion.put(keyO,createValue(objectType, value));
			} else {
				out.println("Usage: put name {value=n [int|long|float|double|str|obj|usr]|valuesize=n} [keytype={int|str}]");
			}
		}
	}
	
	/**
	 * mput {keytype=[int|str] keyrange=n-m [base=\"AAA BBB\"] | k1 k2 .. kn} [value=n [valuetype=int|long|str|obj|usr] | valuesize=n] [seqid=x] [waittime=y]
	 * If valuetype is omited, it will be str. If valuerange is omited, it will be the same as keyrange. 
	 * If valuesize is specified, the valuetype should be str. Otherwise it will throw exception.
	 *
	 */
	void mput(String command) throws CacheException {
		LinkedList keys = new LinkedList();
		LinkedList values = new LinkedList();
		boolean status = getKeyValueList(command, keys, values);
		if (status == false) {
			out.println("Usage: mput {keytype=[int|str] keyrange=n-m [base=\"AAA BBB\"] | k1 k2 .. kn} [value=n [valuetype=int|long|float|double|str|obj|usr] | valuesize=n] [seqid=x] [waittime=y]");
			return;
		}
		Object o = keys.get(0);
		if (o instanceof String && ((String)o).equals("ALL_KEYS")) {
			out.println("mput cannot use ALL_KEYS");
			return;
		}

		long runtime = -1; // for case that runtime is not specified
		if (waittime != null) {
			runtime = Long.parseLong(waittime);
			runtime *= 60000;
			waittime = null;
		}

		long ts1 = System.currentTimeMillis();
		Date date = new Date(ts1);
		out.println("Start Time: "+ts1+" Date: "+date.toString());
		int cnt = 0;
		long ts2 = 0;
		while (runtime != 0) {
			for (int i=0; i<keys.size(); i++) {
				this.currRegion.put(keys.get(i), values.get(i));
				cnt++;
				if (runtime!=-1 && (cnt % 100 == 0)) {
					// check the time every 100 ops
					ts2 = System.currentTimeMillis();
					if (ts2 - ts1 > runtime) {
						runtime = 0;
						break; 
					}
				}
			}
			if (runtime==-1) break; // only run once
		}
		ts2 = System.currentTimeMillis();
		long diff = ts2 - ts1;
		if (diff == 0) diff = 1;
		date = new Date(ts2);
		out.println("End Time: "+ts2+" Date: "+date.toString());
		if (seqid != null) {
			out.println("mput seqid "+seqid+" finished. Consumed: "+diff+" ms. "+cnt+" ops, "+cnt*1000/diff+ " ops/sec.");
			seqid = null;
		}
	}
	
	private String initStrKey(String keyBase, int value) {
		StringBuffer sb = new StringBuffer();
		String s = Integer.toString(value);
		if (keyBase == null) {
			return s;
		} else {
			sb.append(keyBase);
			for (int i=0; i<10-s.length(); i++) {
				sb.append(" ");
			}
			sb.append(s);
			return sb.toString();
		}
	}

	private String getKeyType(LinkedList list) {
		String keytype = null;		
		for (int i=0; i<list.size(); i++) {
			String parmstr = (String)list.get(i);
			if (parmstr.startsWith("keytype=")) {
				keytype = new String(parmstr.substring(8));
				list.remove(i);
				break;
			}
		}
		if (keytype == null) {
			keytype = "str";
		}
		if (!keytype.equals("int") && !keytype.equals("str")) {
			// err exit 2: keytype error
			out.println("keytype can only be int or str. Use str instead");
			keytype = "str";
		}
		return keytype;
	}
	
	/**
	 * getKeyValueList
	 * paring {keytype=[int|str] keyrange=n-m [base=\"AAA BBB\"] | k1 k2 .. kn} [value=n [valuetype=int|long|str|obj|usr] | valuesize=n]
	 * The results will be saved into keys and values, 2 linked list 
	 *
	 */
	private boolean getKeyValueList(String command, LinkedList keys, LinkedList values) {
		String op = null;
		if (keys != null) {
			keys.clear();
		}
		if (values != null) {
			values.clear();
		}
		LinkedList list = new LinkedList();
		parseCommand(command, list);
		op = new String((String)list.get(0));
		list.remove(0);
		if (list.size() == 0) {
			// err exit 1: empty
			// if (!op.equals("mget")) {
				// out.println("Usage: "+op+" {keytype={int|str} keyrange=n-m [base=\"AAA BBB\"] | k1 k2 .. kn} [value=n [valuetype=int|long|str|obj|usr] | valuesize=n]");
			// }
			return false;
		}

		// read in all the parameters
		String base = null;
		String keytype = "str";
		String keyrange = null;
		String valuetype = "str";
		String valuesize = null;
		String value = null;
		boolean all_keys = false;
		boolean key_list = false;
		for (int i=0; i<list.size(); i++) {
			String parmstr = (String)list.get(i);
			if (parmstr.startsWith("keytype=")) {
				keytype = new String(parmstr.substring(8));
			} else if (parmstr.startsWith("base=")) {
				base = new String(parmstr.substring(5));
			} else if (parmstr.startsWith("keyrange=")) {
				keyrange = new String(parmstr.substring(9));
			} else if (parmstr.startsWith("valuetype=")) {
				valuetype = new String(parmstr.substring(10));
			} else if (parmstr.startsWith("valuesize=")) {
				valuesize = new String(parmstr.substring(10));
			} else if (parmstr.startsWith("value=")) {
				value = new String(parmstr.substring(6));
			} else if (parmstr.startsWith("seqid=")) {
				seqid = new String(parmstr.substring(6));
			} else if (parmstr.startsWith("waittime=")) {
				waittime = new String(parmstr.substring(9));
			} else if (parmstr.startsWith("ALL_KEYS")) {
				all_keys = true;
			} else if (parmstr.equals("-l")) {
				boolean des_l = true; // filter out "-l"
			} else {
				// save k1 k2 ... kn to keys
				keys.add(list.get(i));
			}
		}
		if (valuesize != null) {
			// if specified valuesize=, then only create a default byte[] object
			valuetype = "bytes";
			value = valuesize;
		}
		if (keys.size() != 0) {
			// k1 k2 ... kn
			key_list = true;
		}
		if (key_list && base!=null) {
			out.println("There should be no base=xxx if used key list");
			return false;
		}
		if (key_list && keytype.equals("int")) {
			for (int i=0; i<keys.size(); i++) {
				Integer keyO = Integer.valueOf((String)keys.get(i));
				keys.set(i, keyO);
			}
		}

		// validate parameters
		if (!keytype.equals("int") && !keytype.equals("str") && !keytype.equals("obj")) {
			// err exit 2: keytype error
			out.println("keytype can only be int or str or obj");
			return false;
		}
		if (!keytype.equals("str") && (base != null || all_keys )) {
			// err exit 3: key type error
			out.println("keytype should be str if specified base, or ALL_KEYS");
			return false;
		}
		if (!valuetype.equals("str") && !valuetype.equals("obj") && !valuetype.equals("int") && !valuetype.equals("bytes") && !valuetype.equals("usr") && !valuetype.equals("long")&&!valuetype.equals("float")&&!valuetype.equals("double")) { 
			// err exit 4: valuetype error
			out.println("valuetype can only be int, str or obj");
			return false;
		}

		int kr_low = 0;
		int kr_high = 0;
		int idx = -1;
		if (keyrange!=null) {
			idx = keyrange.indexOf('-');
			if (idx == -1) {
			// err exit 4: key range error
				out.println("keyrange should be in format of n-m, n<=m");
				return false;
			}
			kr_low = Integer.parseInt(keyrange.substring(0,idx));
			kr_high = Integer.parseInt(keyrange.substring(idx+1));
			if (key_list || all_keys) {
				// err exit 5: key type error
				out.println("keyrange cannot be specified together with key list or ALL_KEYS");
				return false;
			}
		}
		if (kr_low > kr_high) {
			// err exit 6: key range error
			out.println("keyrange should be in format of n-m, n<=m");
			return false;
		}
		int valuesize_num = 0; 
		if (valuesize!=null) { 
			valuesize_num = Integer.parseInt(valuesize);
		}
		if (valuesize_num > 0 && !valuetype.equals("bytes")) {
			// err exit 6: valuesize error
			out.println("If specified valuesize, the valuetype has to be bytes only");
			return false;
		}
		if (valuesize_num > 65535) {
			// err exit 7: valuesize error
			out.println("value size should be less than 64K");
			return false;
		}

		if (!key_list) {
			if (keytype.equals("int")) {
				keys.clear();
				for (int i=kr_low; i<kr_high; i++) {
					Integer key = new Integer(i);
					keys.add(key);
				}
			} else if (keytype.equals("obj")) {
				keys.clear();
				for (int i=kr_low; i<kr_high; i++) {
					ExampleObject key = new ExampleObject(i);
					keys.add(key);
				}
			} else {
				for (int i=kr_low; i<kr_high; i++) {
					keys.add(initStrKey(base, i));
				}
			}
		}
		
		if (all_keys) {
			keys.clear();
			keys.add("ALL_KEYS");
			key_list = false;
			
			// special handling for ALL_KEYS on mverify
			// we only need to create one object, which could be int|long|str|bytes|usr|obj
			if (op.equals("mverify")) {
				values.clear();
				values.add(createValue(valuetype, value));
			}
		} else {
		// only specify valuelist when mput or mverify and !all_keys
			if (op.equals("mput") || op.equals("mverify")) {
				if (key_list) {
					kr_low = 0;
					kr_high = keys.size();
				}
				for (int i=kr_low; i<kr_high; i++) {
					values.add(createValue(valuetype, value));
				}
				if (keys.size() != values.size()) {
					out.println("Sizes of keys and values are mismatch(): "+keys.size()+","+values.size());
				}
			} else {
				values.clear();
			}
		}
		return true;
	}

	/**
	 * verify name {value=n [int|long|str|obj|usr|bytes] | valuesize=n}
	 * verify the value with that in region
	 *
	 * @see Region#put
	 */
	void verify(String command) throws CacheException {
		LinkedList list = new LinkedList();
		parseCommand(command, list);
		if (list.size() < 3 ) {
			out.println("Usage: verify name {value=n [int|long|str|obj|usr|bytes] | valuesize=n}");
			return;
		} else {
			String name = (String) list.get(1);
			Object valueInRegion = null;
			try {
				valueInRegion = this.currRegion.get(name);
			} catch (CacheLoaderException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (TimeoutException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			
			String value = null;
			String valuesize = null;
			String valuetype = null;
			String s = (String) list.get(2);
			if (list.size()>3) {
				valuetype = (String)list.get(3);
			}
			if (valuetype == null) {
				valuetype = "str";
			}
			if (s.startsWith("valuesize=")) {
				valuesize = s.substring(10);
				value = valuesize;
				valuetype = "bytes";
			} else if (s.startsWith("value=")) {
				value = s.substring(6);
				valuesize = value;
			} else {
				out.println("Usage: verify name {value=n [int|long|str|obj|usr|bytes] | valuesize=n}");
				return;
			}
				
			Object valueSpecified = createValue(valuetype, value);
			boolean status = compare2Objects(valueInRegion, valueSpecified);
			if (status == false) {
				out.println("Mismatch in key:"+name);
			}
		}
	}

	/**
	 * compare2Obects
	 * verify the value with that in region
	 *
	 * @see Region#put
	 */
	private boolean compare2Objects(Object valueInRegion, Object valueSpecified) {
		try {
		if (valueInRegion instanceof String) {
			String s1 = (String)valueSpecified;
			String s2 = (String)valueInRegion;
			if (s1.equals(s2)) {
				return true;
			} else {
				out.println("Mismatch in string, expect:"+s1+",actual:"+s2);
				return false;
			}
		} else if (valueInRegion instanceof Integer) {
			Integer i1 = (Integer)valueSpecified;
			Integer i2 = (Integer)valueInRegion;
			if (i1.equals(i2)) {
				return true;
			} else {
				out.println("Mismatch in int, expect:"+i1+",actual:"+i2);
				return false;
			}
		} else if (valueInRegion instanceof Long) {
			Long l1 = (Long)valueSpecified;
			Long l2 = (Long)valueInRegion;
			if (l1.equals(l2)) {
				return true;
			} else {
				out.println("Mismatch in long, expect:"+l1+",actual:"+l2);
				return false;
			}
		} else if (valueInRegion instanceof Float) {
			Float l1 = (Float)valueSpecified;
			Float l2 = (Float)valueInRegion;
			if (l1.equals(l2)) {
				return true;
			} else {
				out.println("Mismatch in Float, expect:"+l1+",actual:"+l2);
				return false;
			}
		} else if (valueInRegion instanceof Double) {
			Double l1 = (Double)valueSpecified;
			Double l2 = (Double)valueInRegion;
			if (l1.equals(l2)) {
				return true;
			} else {
				out.println("Mismatch in Double, expect:"+l1+",actual:"+l2);
				return false;
			}
		} else if (valueInRegion instanceof User) {
			User u1 = (User)valueSpecified;
			User u2 = (User)valueInRegion;
			if (u1.equals(u2)) {
				return true;
			} else {
				out.println("Mismatch in usr, expect:"+u1+",actual:"+u2);
				return false;
			}
		} else if (valueInRegion instanceof ExampleObject) {
			ExampleObject e1 = (ExampleObject)valueSpecified;
			ExampleObject e2 = (ExampleObject)valueInRegion;
			if (e1.equals(e2)) {
				return true;
			} else {
				out.println("Mismatch in obj, expect:"+e1+",actual:"+e2);
				return false;
			}
		} else if (valueInRegion instanceof byte[]) {
			byte[] v1 = (byte[])valueSpecified;
			byte[] v2 = (byte[])valueInRegion;
			if (v1.length == v2.length) {
				return true;
			} else {
				out.println("Mismatch in length, expect:"+v1.length+", actual:"+v2.length);
				return false;
			}
		} else {
			if (valueInRegion!=null) {
				out.println("Unexpected data type, expect:"+valueSpecified.toString()+", actual:"+valueInRegion.toString());
			} else {
				out.println("Unexpected null value in Region.");
			}
			String s1 = String.valueOf(valueInRegion);
			String s2 = String.valueOf(valueSpecified);
			return s1.equals(s2);
		}
		} catch (java.lang.ClassCastException ex) {
			out.println("Mismatch in type: "+ex.getMessage());
			return false;
		}
	}

	/**
	 * mverify keytype={int|str} {keyrange=n-m [base=\"AAA BBB\"] | k1 k2 .. kn} {value=n [valuetype=int|long|str|obj|usr|bytes] | valuesize=n}
	 * multiple verify the value with that in region
	 *
	 * @see Region#put
	 */
	void mverify(String command) {
		LinkedList keys = new LinkedList();
		LinkedList values = new LinkedList();
		boolean status = getKeyValueList(command, keys, values);
		if (status == false) {
			out.println("Usage:mverify keytype={int|str} {keyrange=n-m [base=\"AAA BBB\"] | k1 k2 .. kn} {value=n [valuetype=int|long|float|double|str|obj|usr|bytes] | valuesize=n}");
			return;
		}
		long ts1 = System.currentTimeMillis();
		Date date = new Date(ts1);
		out.println("Start Time: "+ts1+" Date: "+date.toString());
		Object o = keys.get(0);
		if (o instanceof String && ((String)o).equals("ALL_KEYS")) {
			status = true;
			Iterator it = this.currRegion.keys().iterator();
			while (it.hasNext()) {
				Object entryName = it.next();
				Object valueBytes = null;
				try {
					valueBytes = this.currRegion.get(entryName);
				} catch (CacheLoaderException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (TimeoutException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				status = compare2Objects(valueBytes, values.get(0));
				if (status == false) {
					out.println("Mismatch at key:"+entryName);
					break;
				}
			}
		} else { // not all keys
			status = true;
			for (int i=0; i<keys.size(); i++) {
				Object entryName = keys.get(i);
				Object valueBytes = null;
				try {
					valueBytes = this.currRegion.get(entryName);
				} catch (CacheLoaderException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (TimeoutException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				status = compare2Objects(valueBytes, values.get(i));
				if (status == false) {
					out.println("Mismatch at key:"+entryName);
					break;
				}
			}
		}
		if (status == true) {
			out.println("All keys matched");
		}
		long ts2 = System.currentTimeMillis();
		long diff = ts2 - ts1;
		if (diff == 0) diff = 1;
		date = new Date(ts2);
		out.println("End Time: "+ts2+" Date: "+date.toString());
		if (seqid != null) {
			out.println("mverify seqid "+seqid+" finished. Consumed: "+diff+" ms. "+keys.size()+" ops, "+keys.size()*1000/diff+ " ops/sec.");
			seqid = null;
		}
	}
	
	/**
	 * mget keytype=[int|str] [base=\"AAA BBB\"] keyrange=n-m | k1 k2 .. kn [seqid=x] [waittime=y]");
	 * Get values of multiple keys.
	 *
	 */
	void mget(String command) throws CacheException {
		LinkedList keys = new LinkedList();
		LinkedList values = new LinkedList();
		boolean status = getKeyValueList(command, keys, values);
		if (status == false) {
			out.println("Usage: mget keytype=[int|str] [base=\"AAA BBB\"] keyrange=n-m | k1 k2 .. kn [seqid=x] [waittime=y]");
			return;
		}
		Object o = keys.get(0);
		if (o instanceof String && ((String)o).equals("ALL_KEYS")) {
			out.println("mget cannot use ALL_KEYS");
			return;
		}
		
		long runtime = -1; // for case that runtime is not specified 
		if (waittime != null) {
			runtime = Long.parseLong(waittime);
			runtime *= 60000;
			waittime = null;
		}
		
		long ts1 = System.currentTimeMillis();
		Date date = new Date(ts1);
		out.println("Start Time: "+ts1+" Date: "+date.toString());
		int cnt = 0;
		long ts2 = 0;
		while (runtime != 0) {
			for (int i=0; i<keys.size(); i++) {
				Object valueBytes = this.currRegion.get(keys.get(i));
				if (this.echo) 	printEntry(keys.get(i), valueBytes);
				cnt++;
				if (runtime!=-1 && (cnt % 100 == 0)) {
					// check the time every 100 ops
					ts2 = System.currentTimeMillis();
					if ((ts2 - ts1) > runtime) {
						runtime = 0;
						break; 
					}
				}
			}
			if (runtime==-1) break; // only run once
		}
		ts2 = System.currentTimeMillis();
		long diff = ts2 - ts1;
		if (diff == 0) diff = 1;
		date = new Date(ts2);
		out.println("End Time: "+ts2+" Date: "+date.toString());
		if (seqid != null) {
			out.println("mget seqid "+seqid+" finished. Consumed: "+diff+" ms. "+cnt+" ops, "+cnt*1000/diff+ " ops/sec.");
			seqid = null;
		}
	}
	
	/**
	 * mdes [-l] {k1 k2 ... kn | keytype={int|str} [base=\"AAA BBB\"] keyrange=n-m}");
	 * destroy values of multiple keys. If -l is specified, it will only detroy the entries in local region.
	 *
	 */
	void mdes(String command) throws CacheException {
		LinkedList keys = new LinkedList();
		LinkedList values = new LinkedList();
		boolean status = getKeyValueList(command, keys, values);
		if (status == false) {
			out.println("Usage: mdes [-l] {k1 k2 ... kn | keytype={int|str} [base=\"AAA BBB\"] keyrange=n-m}");
			return;
		}
		Object o = keys.get(0);
		if (o instanceof String && ((String)o).equals("ALL_KEYS")) {
			out.println("mdes cannot use ALL_KEYS");
			return;
		}

		// check if there's -l
		LinkedList list = new LinkedList();
		parseCommand(command, list);
		boolean des_l= false;
		for (int i=1; i<list.size(); i++) {
			String parmstr = (String)list.get(i);
			if (parmstr.equals("-l")) {
				des_l = true;
				break;
			}
		}
		
		long ts1 = System.currentTimeMillis();
		Date date = new Date(ts1);
		out.println("Start Time: "+ts1+" Date: "+date.toString());
		for (int i=0; i<keys.size(); i++) {
			Object valueBytes = this.currRegion.get(keys.get(i));
			// printEntry(keys.get(i), valueBytes);
			if (des_l) {
				this.currRegion.localDestroy(keys.get(i));
			} else {
				this.currRegion.destroy(keys.get(i));
			}
		}
		long ts2 = System.currentTimeMillis();
		long diff = ts2 - ts1;
		if (diff == 0) diff = 1;
		date = new Date(ts2);
		out.println("End Time: "+ts2+" Date: "+date.toString());
		if (seqid != null) {
			out.println("mdes seqid "+seqid+" finished. Consumed: "+diff+" ms. "+keys.size()+" ops, "+keys.size()*1000/diff+ " ops/sec.");
			seqid = null;
		}
	}
	
	/**
	 * minv [-l] {k1 k2 ... kn | keytype={int|str} [base=\"AAA BBB\"] keyrange=n-m}");
	 * invalidate values of multiple keys. If -l is specified, it will only detroy the entries in local region.
	 *
	 */
	void minv(String command) throws CacheException {
		LinkedList keys = new LinkedList();
		LinkedList values = new LinkedList();
		boolean status = getKeyValueList(command, keys, values);
		if (status == false) {
			out.println("Usage: minv [-l] {k1 k2 ... kn | keytype={int|str} [base=\"AAA BBB\"] keyrange=n-m}");
			return;
		}
		Object o = keys.get(0);
		if (o instanceof String && ((String)o).equals("ALL_KEYS")) {
			out.println("minv cannot use ALL_KEYS");
			return;
		}

		// check if there's -l
		LinkedList list = new LinkedList();
		parseCommand(command, list);
		boolean des_l= false;
		for (int i=1; i<list.size(); i++) {
			String parmstr = (String)list.get(i);
			if (parmstr.equals("-l")) {
				des_l = true;
				break;
			}
		}

		long ts1 = System.currentTimeMillis();
		Date date = new Date(ts1);
		out.println("Start Time: "+ts1+" Date: "+date.toString());
		for (int i=0; i<keys.size(); i++) {
			Object valueBytes = this.currRegion.get(keys.get(i));
			printEntry(keys.get(i), valueBytes);
			if (des_l) {
				this.currRegion.localInvalidate(keys.get(i));
			} else {
				this.currRegion.invalidate(keys.get(i));
			}
		}
		long ts2 = System.currentTimeMillis();
		long diff = ts1 - ts1;
		if (diff == 0) diff = 1;
		date = new Date(ts2);
		out.println("End Time: "+ts2+" Date: "+date.toString());
		if (seqid != null) {
			out.println("minv seqid "+seqid+" finished. Consumed: "+diff+" ms. "+keys.size()+" ops, "+keys.size()*1000/diff+ " ops/sec.");
			seqid = null;
		}
	}
	
	/**
	 * Destroys (local or distributed) a region or entry in the current
	 * region.
	 *
	 * @see Region#destroyRegion
	 */
	void des(String command) throws CacheException {
		
		LinkedList list = new LinkedList();
		parseCommand(command,list);
		String arg1 = null;
		String arg2 = null;
		boolean des_l= false;
		String keytype = getKeyType(list);

		switch (list.size()) {
		case 1:
			// inv followed by nothing invalidates the current region
			this.currRegion.destroyRegion();
			break;
		case 2:
			arg1 = (String) list.get(1);
			des_l= arg1.equals("-l");
			if (des_l) {
				// inv -l local invalidates current region
				this.currRegion.localDestroyRegion();
				
			}
			else {
				// inv name invalidate the entry name in current region
				Object keyO = arg1;
				if (keytype.equals("int")) {
					keyO = Integer.valueOf(arg1);
				}
				this.currRegion.destroy(keyO);				
			}
			break;
		case 3:
			// inv -l name local invalidates name in current region
			arg1 = (String) list.get(1);
			arg2 = (String) list.get(2);
			des_l = arg1.equals("-l");
			if (des_l) {
				Object keyO = arg2;
				if (keytype.equals("int")) {
					keyO = Integer.valueOf(arg2);
				}
				this.currRegion.localDestroy(keyO);				
			}
			break;
		default:
			out.println("Usage: des [-l] [name] [keytype={int|str}]");
			break;
		}
	}
	
	/**
	 * Lists the contents of the current region.
	 *
	 * @see Region#entries
	 */
	void ls(String command) throws CacheException {
		boolean ls_l = command.indexOf("-l") != -1;
		if (ls_l) {
			out.println(this.currRegion.getFullPath() + " attributes:");
			out.println("\t" + this.currRegion.getAttributes());
		}
		out.println("Region Entries:");
		Set nameSet = this.currRegion.entries(false);
		for (Iterator itr = nameSet.iterator(); itr.hasNext();) {
			Region.Entry entry = (Region.Entry) itr.next();
			Object name = (Object)entry.getKey();
			Object valueBytes = entry.getValue();
			printEntry(name, valueBytes);
			
			if (ls_l) {
				out.println("\t\t" + this.currRegion.get(name));
			}
		}
		out.println();
		out.println("Subregions:");
		Set regionSet = this.currRegion.subregions(false);
		for (Iterator itr = regionSet.iterator(); itr.hasNext();) {
			Region rgn = (Region)itr.next();
			String name = rgn.getName();
			out.println("\t" + name);
			if(rgn.getAttributes().getStatisticsEnabled()) {
				CacheStatistics stats = rgn.getStatistics();
				out.println("\n\t\t" + stats);
			}
		}
	}
	
	/**
	 * Lists the map of region attributes templates that are stored 
	 * in the cache
	 */
	void lsAtts(String command) throws CacheException {
		Map attMap = this.cache.listRegionAttributes();
		Iterator itrKeys = attMap.keySet().iterator(); 
		for (Iterator itrVals = attMap.values().iterator(); itrVals.hasNext();) {
			RegionAttributes rAtts = (RegionAttributes)itrVals.next();
			out.println("\tid=" + itrKeys.next() + "; scope=" + rAtts.getScope() + "; mirrorType=" + rAtts.getMirrorType() + "\n");
		}
	}
	
	/**
	 * Prints the key/value pair for an entry 
	 * This method recognizes a subset of all possible object types. 
	 */
	void printEntry(Object key, Object valueBytes) {
		String value = "null";
		
		if (valueBytes != null) {
			if (valueBytes instanceof byte[]) {
				byte[] v = (byte[])valueBytes;
				int len = v.length;
				if (len > 20) len = 20;
				value = new String("byte[]: \"" + new String((byte[]) valueBytes, 0, len) + "\"");
			}
			else if (valueBytes instanceof String) {
				String v = (String)valueBytes;
				int len = v.length();
				if (len > 20) len = 20;

				value = new String("String: \"" + ((String)valueBytes).substring(0,len) + "\"");
			}
			else if (valueBytes instanceof Integer) {
				value = new String("Integer: \"" + valueBytes.toString() + "\"");
			}
			else if (valueBytes instanceof Long) {
				value = new String("Long: \"" + valueBytes.toString() + "\"");
			}
			else if (valueBytes instanceof Float) {
				value = new String("Float: \"" + valueBytes.toString() + "\"");
			}
			else if (valueBytes instanceof Double) {
				value = new String("Double: \"" + valueBytes.toString() + "\"");
			}
			else if (valueBytes instanceof ExampleObject) {
				ExampleObject parseObject = (ExampleObject)(valueBytes);
				value = new String(parseObject.getClass().getName() +
						": \"" + parseObject.getDouble_field() +
						"\"(double)"
						+ " \"" + parseObject.getLong_field() + "\"(long)"
						+ " \"" + parseObject.getFloat_field() + "\"(float)"
						+ " \"" + parseObject.getInt_field() + "\"(int)"
						+ " \"" + parseObject.getShort_field() + "\"(short)"
						+ " \"" + parseObject.getString_field() + "\"(String)");
				Vector v = parseObject.getString_vector();
				value += "\"" + v.toString() + "\"(String Vector)";
			}
			else if (valueBytes instanceof User) {
				User parseObject = (User)(valueBytes);
				value = new String(parseObject.getClass().getName() +
						": \"" + parseObject.getUserId() + "\"(userId)"
						+ " \"" + parseObject.getName() + "\"(userName)");
				ExampleObject eo = parseObject.getEO();
				String valueEO = new String(eo.getClass().getName() +
						": \"" + eo.getDouble_field() + "\"(double)" 
						+ " \"" + eo.getLong_field() + "\"(long)"
						+ " \"" + eo.getFloat_field() + "\"(float)"
						+ " \"" + eo.getInt_field() + "\"(int)"
						+ " \"" + eo.getShort_field() + "\"(short)"
						+ " \"" + eo.getString_field() + "\"(String)");
				Vector v = eo.getString_vector();
				valueEO += "\"" + v.toString() + "\"(String Vector)";
				value += "\n" + valueEO;
			}
			else if (valueBytes instanceof byte[]) {
				byte[] parseObject = (byte[])(valueBytes);
				value = new String(parseObject.getClass().getName() 
						+ " \"" + parseObject.toString() + "\"(content)");
				value += "\n";
			}
			else {
				value = String.valueOf(valueBytes);
			}
		}
		else {
			value = new String ("No value in cache."); 
		}
		if (key instanceof Integer) {
			out.println("     " + key + " -> " + value);
		} else {
			out.println("     \"" + key + "\" -> " + value);
		}
	}
	
	/**
	 * Sets an expiration attribute of the current region
	 *
	 * @see Region#getAttributesMutator
	 */
	void setExpirationAttr(String command) throws Exception {
		AttributesMutator mutator = this.currRegion.getAttributesMutator();
		LinkedList list = new LinkedList();
		parseCommand(command,list);
		ExpirationAttributes attributes = null;
		int time = 0;
		int index  = -1;;
		
		// Item 0 is "set" and item 1 is "expiration"
		ListIterator commListIter = list.listIterator(2);
		if (!commListIter.hasNext()) {
			out.println("set expiration must have an attribute and a value");
			return;
		} 
		String attrName = (String)commListIter.next();
		
		String attrValue = null;
		if (!commListIter.hasNext()) {
			out.println("set expiration attributes must have either a numeric value or null (no expiration)");
			return;
		} 
		attrValue = (String)commListIter.next();
		if (attrValue.equalsIgnoreCase("null")) {
			time = -1;
			if (commListIter.hasNext()) {
				out.println("null expiration attributes can not have an action");	  
				return;
			}
		} else {
			if ((time = parseInt(attrValue)) < 0) {
				out.println("Attribute values are either an integer or the string null");	  
				return;
			}
		}
		
		String attrAction = null;
		if (commListIter.hasNext()) {
			attrAction = (String)commListIter.next();
		}
		
		if (attrName.startsWith("regionIdleTime")) {
			if (time==-1) {
				mutator.setRegionIdleTimeout(new ExpirationAttributes(0));
			} else {
				if (null==(attributes=parseExpAction(time,attrAction))) {
					return;
				}
				mutator.setRegionIdleTimeout(attributes);
			}
		} else if (attrName.startsWith("entryIdleTime")) {
			if (time==-1) {
				mutator.setEntryIdleTimeout(new ExpirationAttributes(0));
			} else {
				if (null==(attributes=parseExpAction(time,attrAction))) {
					return;
				}
				mutator.setEntryIdleTimeout(attributes);
			}
		} else if (attrName.startsWith("entryTTL")) {
			if (time==-1) {
				mutator.setEntryTimeToLive(new ExpirationAttributes(0));
			} else {
				if (null==(attributes=parseExpAction(time,attrAction))) {
					return;
				}
				mutator.setEntryTimeToLive(attributes);
			}
		} else if (attrName.startsWith("regionTTL")) {
			if (time==-1) {
				mutator.setRegionTimeToLive(new ExpirationAttributes(0));
			} else {
				if (null==(attributes=parseExpAction(time,attrAction))) {
					return;
				}
				mutator.setRegionTimeToLive(attributes);
			}
		} else {
			out.println("Unrecognized expiration attribute name: " + attrName);
			return;
		}
	}
	
	/**
	 * Sets a region attribute of the current region
	 *
	 * @see #setExpirationAttr
	 * @see Region#getAttributesMutator
	 */
	void setRgnAttr(String command) throws Exception {
		AttributesMutator mutator = this.currRegion.getAttributesMutator();
		String name = parseName(command);
		if (name == null) {
			return;
		}
		
		if (name.indexOf("exp") != -1) {
			setExpirationAttr(command);
			return;
		}
		
		final String value = parseValue(command);
		if (name.equals("loader")) {
			if (value != null && value.equalsIgnoreCase("null"))
				mutator.setCacheLoader(null);
			else
				mutator.setCacheLoader(new LoggingCacheLoader());
		}
		else if (name.equals("listener")) {
			if (value != null && value.equalsIgnoreCase("null"))
				mutator.setCacheListener(null);
			else
				mutator.setCacheListener(new LoggingCacheListener());
		}
		else if (name.equals("writer")) {
			if (value != null && value.equalsIgnoreCase("null"))
				mutator.setCacheWriter(null);
			else
				mutator.setCacheWriter(new LoggingCacheWriter());
		}
		else if (name.equals("txnlistener")) {
			CacheTransactionManager manager =
				this.currRegion.getCache().getCacheTransactionManager();
			if (value != null && value.equalsIgnoreCase("null"))
				manager.setListener(null);
			else
				manager.setListener(new LoggingTransactionListener());
		}
		else {
			out.println("Unrecognized attribute name: " + name);
		}
	}
	
	/**
	 * Specifies the <code>cache.xml</code> file to use when creating
	 * the <code>Cache</code>.  If the <code>Cache</code> has already
	 * been open, then the existing one is closed.
	 *
	 * @see CacheFactory#create
	 */
	void load(String command) throws CacheException {
		String name = parseName(command);
		if (name != null) {
			Properties env = new Properties();
			env.setProperty("cache-xml-file", name);
			system = DistributedSystem.connect(env);
			if (system != null) {
				if (cache != null)
					cache.close();
				system.disconnect();
			}
			this.cache = CacheFactory.create(system);
			currRegion = cache.getRegion("root");
			// pendRegionAttributes = currRegion.getAttributes();
		}
	}
	
	/**
	 * Opens the <code>Cache</code> and sets the current region to the
	 * "root" region.
	 *
	 * @see Cache#getRegion
	 */
	void open(String command) throws Exception {
		if (system != null) {
			if (cache != null)
				cache.close();
			system.disconnect();
			
		}
		Properties env = new Properties();
		this.cache = CacheFactory.create(DistributedSystem.connect(env));
		currRegion = cache.getRegion("root");
	}
	
	/**
	 * Begins a transaction in the current thread
	 */
	void begin(String command) {
		if (this.cache == null) {
			String s = "The cache is not currently open";
			out.println(s);
			return;
		}
		
		CacheTransactionManager manager =
			this.cache.getCacheTransactionManager();
		manager.begin();
	}
	
	/**
	 * Commits the transaction associated with the current thread.  If
	 * the commit fails, information about which region entries caused
	 * the conflict can be obtained from the {@link
	 * TransactionListener}.
	 */
	void commit(String command) {
		if (this.cache == null) {
			String s = "The cache is not currently open";
			out.println(s);
			return;
		}
		
		CacheTransactionManager manager =
			this.cache.getCacheTransactionManager();
		try {
			manager.commit();
			
		} catch (CommitConflictException ex) {
			String s = "While committing transaction";
			out.println(s + ": " + ex);
		}
	}
	
	/**
	 * Rolls back the transaction associated with the current thread
	 */
	void rollback(String command) {
		if (this.cache == null) {
			String s = "The cache is not currently open";
			out.println(s);
			return;
		}
		
		CacheTransactionManager manager =
			this.cache.getCacheTransactionManager();
		manager.rollback();
	}
	
	// ************ Parsing methods **********************************
	
	/**
	 * Parses a <code>command</code> and expects that the second token
	 * is a name.
	 */
	private String parseName(String command) {
		int space = command.indexOf(' ');
		if (space < 0) {
			out.println("You need to give a name argument for this command");
			return null;
		}
		else {
			int space2 = command.indexOf(' ', space+1);
			if (space2 < 0)
				return command.substring(space+1);
			else
				return command.substring(space+1, space2);
		}
	}

	/**
	 * Parses a <code>command</code> and expects that the third token is
	 * a value.
	 */
	private String parseValue(String command) {
		int space = command.indexOf(' ');
		if (space < 0) {
			return null;
		}
		space = command.indexOf(' ', space+1);
		if (space < 0) {
			return null;
		}
		else {
			int space2 = command.indexOf(' ', space+1);
			if (space2 < 0)
				return command.substring(space+1);
			else
				return command.substring(space+1, space2);
		}
	}
	
	/**
	 * Parses an <code>int</code> from a <code>String</code>
	 */
	private int parseInt(String value) {
		try {
			return Integer.parseInt(value);
		}
		catch (Exception e) {
			out.println("illegal number: " + value);
			return -1;
		}
	}
	
	/**
	 * Parses a <code>command</code> and places each of its tokens in a
	 * <code>List</code>.
	 * Tokens are separated by whitespace, or can be wrapped with double-quotes
	 */
	private boolean parseCommand(String command, List list) {
		Reader in = new StringReader(command);
		StringBuffer currToken = new StringBuffer();
		String delim = " \t\n\r\f";
		int c;
		boolean inQuotes = false;
		do {
			try {
				c = in.read();
			}
			catch (IOException e) {
				throw new Error("unexpected exception", e);
			}
			
			if (c < 0) break;
			
			if (c == '"') {
				if (inQuotes) {
					inQuotes = false;
					list.add(currToken.toString());
					currToken = new StringBuffer();
				}
				else {
					inQuotes = true;
				}
				continue;
			}
			
			if (inQuotes) {
				currToken.append((char)c);
				continue;
			}
			
			if (delim.indexOf((char)c) >= 0) {
				// whitespace
				if (currToken.length() > 0) {
					list.add(currToken.toString());
					currToken = new StringBuffer();
				}
				continue;
			}
			
			currToken.append((char)c);
		} while (true);
		
		if (currToken.length() > 0) {
			list.add(currToken.toString());
		}
		return true;
	}
	
	/**
	 * Creates <code>ExpirationAttributes</code> from an expiration time
	 * and the name of an expiration action.
	 */
	private ExpirationAttributes parseExpAction( int expTime,
			String actionName) {
		if (actionName == null) {
			if (expTime > 0) {
				out.println("Setting expiration action to default (invalidate)");
			}
			return new ExpirationAttributes(expTime);
		}
		
		if (actionName.equals("destroy")) {
			return new ExpirationAttributes(expTime, ExpirationAction.DESTROY);
		}
		else if (actionName.startsWith("inv")) {
			return new ExpirationAttributes(expTime, ExpirationAction.INVALIDATE);
		}
		else if (actionName.startsWith("localDes")) {
			return new ExpirationAttributes(expTime, ExpirationAction.LOCAL_DESTROY);
		}
		else if (actionName.startsWith("localInv")) {
			return new ExpirationAttributes(expTime, ExpirationAction.LOCAL_INVALIDATE);
		} else {
			out.println("Expiration Action not understood: " + actionName);
			return null;
		}
	}
	
}
