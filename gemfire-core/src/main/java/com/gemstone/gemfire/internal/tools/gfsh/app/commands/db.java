package com.gemstone.gemfire.internal.tools.gfsh.app.commands;

import java.util.ArrayList;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.tools.gfsh.app.CommandExecutable;
import com.gemstone.gemfire.internal.tools.gfsh.app.Gfsh;
import com.gemstone.gemfire.internal.tools.gfsh.app.util.DBUtil;

/**
 * Bulk copy to/from database.
 * @author dpark
 *
 */
public class db implements CommandExecutable
{
	private Gfsh gfsh;
	
	private DBUtil dbUtil;
	
	public db(Gfsh gfsh)
	{
		this.gfsh = gfsh;
	}

	public void help()
	{
		gfsh.println("db [{<region path> | <oql>} {in | out} {<table name> | <sql>}]");
		gfsh.println("   [-k [<primary column>]]");
		gfsh.println("   [-v [primary column]]");
		gfsh.println("   [-b <batch size>]");
		gfsh.println("db [-driver <jdbc driver>]"); 
		gfsh.println("   [-url <jdbc url>]");
		gfsh.println("   [-u <user name>]");
		gfsh.println("   [-p <password>]");
		gfsh.println("db [-?]");
		gfsh.println("   Load database contents into a region or store region contents to.");
		gfsh.println("   a database table. db has 2 distinctive commands. 'db -driver...'");
		gfsh.println("   to initialize database and 'db region_path...' to load/store ");
		gfsh.println("   from/to database. Note that if the region is a partitioned region");
		gfsh.println("   then the 'out' option retrieves data only from the local dataset");
		gfsh.println("   the connected server due to the potentially large size of the");
		gfsh.println("   partitioend region.");
		gfsh.println();
		gfsh.println("   {<region path> | <oql>} region path or OQL query statement. <region path>");
		gfsh.println("               stores all of the region entries into the database table.");
		gfsh.println("               If <oql>, the query tuples must match table column names.");
		gfsh.println("   {in | out} 'in' for load data into the region from the database");
		gfsh.println("              'out' for store data out to the database from the region.");
		gfsh.println("   {<table name> | <sql>} table name or SQL query statement. <table name>");
		gfsh.println("              loads the entire table contents.");
		gfsh.println();
		gfsh.println("   Requirements:");
		gfsh.println("      The data class must have getter and setter methods for the matching");
		gfsh.println("      query tuples. db supports case-insensitive table column names.");
		gfsh.println("   Examples:");
		gfsh.println("      To connect to a dababase:");
		gfsh.println("         db -driver com.mysql.jdbc.Driver -url jdbc:mysql://localhost/market \\");
		gfsh.println("         -u root -p root");
		gfsh.println("      To store the /prices region entries to the price_table database table:");
		gfsh.println("         db /prices out price_table");
		gfsh.println("      To load database query results to a region:");
		gfsh.println("         db /prices in \"select * from price_table");
		gfsh.println();
	}
	
	public void execute(String command) throws Exception
	{
		if (command.startsWith("db -?")) {
			help();
		} else {
			db(command);
		}
	}
	
	public String getDbInitCommand()
	{
		String dbSettings = null;
		if (dbUtil != null) {
			
			String url = dbUtil.getUrl();
			String driverName = dbUtil.getDriverName();
			String userName = dbUtil.getUserName();
			
			if (url == null || driverName == null) {
				return null;
			}
			
			dbSettings = "db ";
			if (url != null) {
				dbSettings += "-url " + url;
			}
			if (driverName != null) {
				dbSettings += " -driver " + driverName;
			}
			if (userName != null) {
				dbSettings += " -u " + userName;
			}
		}
		return dbSettings;
	}
	
	private void db(String command) throws Exception
	{
		ArrayList<String> list = new ArrayList();
		gfsh.parseCommand(command, list);
		
		
		// options
		boolean optionSpecified = false;
		String driver = null;
		String url = null;
		String user = null;
		String password = null;
		boolean storeKeys = false;
		boolean storeValues = false;
		boolean isKeyPrimary = false;
		boolean isValuePrimary = false;
		String primaryColumn = null;
		int batchSize = 1000;
		for (int i = 0; i < list.size(); i++) {
			String val = list.get(i);
			if (val.equals("-driver")) {
				i++;
				if (list.size() > i) {
					driver = list.get(i);
				}
				optionSpecified = true;
			} else if (val.equals("-url")) {
				i++;
				if (list.size() > i) {
					url = list.get(i);
				}
				optionSpecified = true;
			} else if (val.equals("-u")) {
				i++;
				if (list.size() > i) {
					user = list.get(i);
				}
				optionSpecified = true;
			} else if (val.equals("-p")) {
				i++;
				if (list.size() > i) {
					password = list.get(i);
				}
				optionSpecified = true;
			} else if (val.equals("-k")) {
				storeKeys = true;
				if (list.size() > i+1 && list.get(i+1).startsWith("-") == false) {
					i++;
					primaryColumn = list.get(i);
					isKeyPrimary = true;
				}
			} else if (val.equals("-v")) {
				storeValues = true;
				if (list.size() > i+1 && list.get(i+1).startsWith("-") == false) {
					i++;
					primaryColumn = list.get(i);
					isValuePrimary = true;
				}
			} else if (val.equals("-b")) {
				i++;
				if (list.size() > i) {
					val = list.get(i);
					batchSize = Integer.parseInt(val);
				}
			}
		}
		
		if (optionSpecified) {
			if (driver == null) {
				gfsh.println("Error: -driver not specified.");
				return;
			}
			if (url == null) {
				gfsh.println("Error: -url not specified.");
				return;
			}
			
			dbUtil = DBUtil.initialize(driver, url, user, password);
			if (dbUtil != null) {
				gfsh.println("Database connected.");
			}
			return;
		}
    
    if (dbUtil == null) {
      gfsh.println("Error: Not connected to database.");
      return;
    }
		
		// Data load/store
		
		// Parse command inputs
		if (list.size() < 4) {
			gfsh.println("Error: incomplete db command. Run db -? for help.");
			return;
		} 
		String fullPath = null;
		String regionPath = list.get(1);
		String directionType = list.get(2);
		
		
		// region 
		Cache cache = CacheFactory.getAnyInstance();
		Region region = null;
		String oql = null;
		if (regionPath.startsWith("select ") == false) {
			regionPath = gfsh.getFullPath(regionPath, gfsh.getCurrentPath());
			region = cache.getRegion(regionPath);
			if (region == null) {
				gfsh.println("Error: region undefined - " + regionPath);
				return;
			}
		} else {
			oql = regionPath;
		}
		
		// in | out
		boolean isIn = false;
		if (directionType.equalsIgnoreCase("in")) {
			isIn = true;
		} else if (directionType.equalsIgnoreCase("out")) {
			isIn = false;
		} else {
			gfsh.println("Error: invalid direction type - " + directionType);
			return;
		}
		
		// table or query
		
		if (isIn) {
			String sql = list.get(3); // sql can be a table name. if table, select * is performed
			dbIn(region, sql, primaryColumn);
		} else {
			String tableName = list.get(3);
			if (storeKeys == false && storeValues == false) {
				storeValues = true;
			}
			
			// value primary overrides key primary. 
			// cannot have two primary columns. only one primary allowed.
			if (isValuePrimary) {
				isKeyPrimary = false;
			}
			int storeType = DBUtil.TYPE_VALUES;
			if (storeKeys && storeValues) {
				storeType = DBUtil.TYPE_KEYS_VALUES;
			} else if (storeKeys) {
				storeType = DBUtil.TYPE_KEYS;
			}
			dbOut(regionPath, tableName, storeType, primaryColumn, isKeyPrimary, batchSize);
		}
	}
	
	private void dbIn(Region region, String sql, String primaryColumn) throws Exception
	{
		long startTime = System.currentTimeMillis();
		int rowCount = dbUtil.loadDB(gfsh, region, gfsh.getQueryKeyClass(), gfsh.getValueClass(), sql, primaryColumn);
		long stopTime = System.currentTimeMillis();
		
		gfsh.println("db in complete");
		gfsh.println("       To (region): " + region.getFullPath());
		gfsh.println("   From (database): " + sql);
		gfsh.println("         Row count: " + rowCount);
		if (gfsh.isShowTime()) {
			gfsh.println("    elapsed (msec): " + (stopTime - startTime));
		}
	}	

//	private void dbOut(Region region, String tableName, 
//			int storeType, String primaryColumn, 
//			boolean isKeyPrimary) throws Exception
//	{
//		long startTime = System.currentTimeMillis();
//		int rowCount = dbUtil.storeDB(gfsh, region, tableName, storeType, primaryColumn, isKeyPrimary);
//		long stopTime = System.currentTimeMillis();
//
//		gfsh.println("db out complete");
//		gfsh.println("   Copied from: " + region.getFullPath());
//		gfsh.println("     Copied to: " + tableName);
//		gfsh.println("     Row count: " + rowCount);
//		if (gfsh.isShowTime()) {
//			gfsh.println("elapsed (msec): " + (stopTime - startTime));
//		}	
//	}
//	
	private void dbOut(String regionPath, String tableName, 
			int storeType, String primaryColumn, 
			boolean isKeyPrimary, int batchSize) throws Exception
	{
		long startTime = System.currentTimeMillis();
		int rowCount = dbUtil.storeDB(gfsh, regionPath, tableName, storeType, primaryColumn, isKeyPrimary, batchSize);
		long stopTime = System.currentTimeMillis();

		if (rowCount == -1) {
			return;
		}
		
		gfsh.println("db out complete");
		gfsh.println("   From (region): " + regionPath);
		gfsh.println("   To (database): " + tableName);
		gfsh.println("       Row count: " + rowCount);
		
		if (gfsh.isShowTime()) {
			gfsh.println("  elapsed (msec): " + (stopTime - startTime));
		}
		
	}
}
