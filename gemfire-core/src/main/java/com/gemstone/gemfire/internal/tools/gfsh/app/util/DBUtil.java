/*=========================================================================
 * Copyright (c) 2008, Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.internal.tools.gfsh.app.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.cache.query.types.CollectionType;
import com.gemstone.gemfire.cache.query.types.ObjectType;
import com.gemstone.gemfire.cache.query.types.StructType;
import com.gemstone.gemfire.internal.tools.gfsh.app.Gfsh;
import com.gemstone.gemfire.internal.tools.gfsh.app.command.task.QueryResults;
import com.gemstone.gemfire.internal.tools.gfsh.app.command.task.QueryTask;
import com.gemstone.gemfire.internal.tools.gfsh.app.misc.util.ReflectionUtil;
import com.gemstone.gemfire.internal.tools.gfsh.command.CommandResults;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
/**
 * DBUtil is a simple database utility class that updates the database
 * with the RBC TradeCapture specific objects.
 * 
 * @author dpark
 */
public class DBUtil
{
	public static final int TYPE_KEYS = 0;
	public static final int TYPE_VALUES = 1;
	public static final int TYPE_KEYS_VALUES = 2;
	
	
	private static DBUtil dbutil;
	
    static SimpleDateFormat dateFormat = new SimpleDateFormat(
            "MM/dd/yyyy HH:mm:ss.SSS");
    static SimpleDateFormat smallDateFormat = new SimpleDateFormat(
    "MM/dd/yy");

    protected Connection conn;
    private String driverName;
    private String url;
    private String userName;
    

    /**
     * Creates a DBUtil object that provides access to the database.
     *
     * @param driverName The JDBC dirver name. For example,
     *                   com.nywe.db.sybaseImpl.SybaseDBConn.
     * @throws DBUtilException Thrown if the driver does not exist.
     */
    private DBUtil(String driverName) throws DBUtilException
    {
        init(driverName);
    }
    
    @SuppressFBWarnings(value="LI_LAZY_INIT_UPDATE_STATIC",justification="This looks like singleton and at present looks like desired functionality")
    public static DBUtil initialize(String driverName, String url, String userName,
            String password) throws DBUtilException
    {
    	if (dbutil != null) {
    		dbutil.close();
    	}
    	dbutil = new DBUtil(driverName);
    	dbutil.connect(url, userName, password);
    	return dbutil;
    }
    
    public static DBUtil getDBUtil()
    {
    	return dbutil;
    }

    /**
     * Initializes DB.
     * @param driverName The name of the JDBC Driver. For example,
     *                   com.nywe.db.sybaseImpl.SybaseDBConn.
     */
    private void init(String driverName) throws DBUtilException
    {
        try {
            // finds and loads the driver dynamically
            Driver driver = (Driver) Class.forName(driverName).newInstance();
            DriverManager.registerDriver(driver);
            this.driverName = driverName;
        } catch (Exception ex) {
            throw new DBUtilException(ex);
        }
    }

    /**
     * Establishes connection to the database server.
     * @param url The database URL.
     * @param userName The user name used to login to the database.
     * @param password The password used to login to the database.
     * @throws DBUtilException Thrown if it encounters a database connection error.
     */
    private synchronized void connect(String url, String userName,
        String password) throws DBUtilException
    {
        if (conn != null) {
            throw new DBUtilException(DBUtilException.ERROR_CONNECTION_ALREADY_ESTABLISHED,
                                      "The database connection has already been established. To establish a new connection, close it first.");
        }

        Properties props = new Properties();
        props.put("user", userName);
        props.put("password", password);

        try {
            conn = DriverManager.getConnection(url, props);
            this.url = url;
            this.userName = userName;
        } catch (SQLException ex) {
            ex.printStackTrace();
            System.out.println("Error Code: " + ex.getErrorCode());
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new DBUtilException(ex);
        }
    }

    public String getDriverName()
	{
		return driverName;
	}

	public String getUrl()
	{
		return url;
	}

	public String getUserName()
	{
		return userName;
	}

	/**
     * Cloases the database connection and frees all system resources used
     * by DBUtil.
     *
     * @throws DBUtilException Thrown if it encounters a database communcations
     *                         error.
     */
    public synchronized void close() throws DBUtilException
    {
        try {
            if (conn != null) {
                conn.close();
                conn = null;
            }
        } catch (Exception ex) {
            throw new DBUtilException(ex);
        }
    }
    
    
    /**
     * Returns true if DBUtil has been closed, otherwise, false. If DBUtil is
     * closed then DBUtil is no longer usable. To reconnect, call connect() again.
     */
    public boolean isClosed()
    {
        return (conn == null);
    }
	
	public int loadDB(Gfsh gfsh, Region region, 
			Class keyClass, 
			Class valueClass, 
			String sql, 
			String primaryColumn) throws DBUtilException 
	{
		int count = 0;
    PreparedStatement statement = null;
		
		try {
			Map<String, Method> keySettersMap = ReflectionUtil.getAllSettersMap(keyClass);
			Map<String, Method> valueSettersMap = ReflectionUtil.getAllSettersMap(valueClass);
			
			// realMap is for mapping methods to case-insensitive table columns
			Map<String, Method> realMap = new HashMap();
			
			if (sql.startsWith("select ") == false) {
				// it's table name. create sql
				sql = "select * from " + sql;
			}

	    	statement = conn.prepareStatement(sql);
	    	ResultSet resultSet = statement.executeQuery();
	    	
    		while (resultSet.next()) {
	            
	            // key
            	Object key = updateObject(gfsh, keyClass, keySettersMap, realMap, resultSet, primaryColumn);
            	
            	// value
            	Object value = updateObject(gfsh, valueClass, valueSettersMap, realMap, resultSet, null);
        	
            	region.put(key, value);
            	
	            count++;
            }
        
    	} catch (Exception ex) {
    		throw new DBUtilException(ex);
    	} finally {
    	  if (statement != null) {
    	    try {
            statement.close();
          } catch (SQLException e) {
            throw new DBUtilException(e);
          }
        }
    	}
    	
    	return count;
	}
    
	public int storeDB(Gfsh gfsh, Region region, String tableName, 
			int storeType, String primaryColumn, 
			boolean isKeyPrimary) throws DBUtilException
	{
		int count = 0;
		try {
			DatabaseMetaData meta = conn.getMetaData();
			ResultSet resultSet = meta.getColumns(null, null, tableName, null);
			ArrayList<ColumnInfo> columnList = new ArrayList();
			while (resultSet.next()) {
				ColumnInfo info = new ColumnInfo();
				info.name = (String)resultSet.getObject("COLUMN_NAME");
				info.type = (Integer)resultSet.getObject("DATA_TYPE"); // java.sql.Types
				columnList.add(info);
			}
			
			// Collect all matching field names from the region object
			Set<Region.Entry> entrySet = region.entrySet();
			
			Map<String, Method> keyGettersMap = new HashMap(0);
			Map<String, Method> valueGettersMap = new HashMap(0);
			int columnCount = 0; 
			String keyKeys[] = new String[0];
			String valueKeys[] = new String[0];
			for (Region.Entry entry : entrySet) {
				Object key = entry.getKey();
				Object value = entry.getValue();
				switch (storeType) {
				case TYPE_KEYS:
					keyGettersMap = ReflectionUtil.getAllGettersMap(key.getClass());
					keyGettersMap = getGettersMap(keyGettersMap, columnList);
					columnCount = keyGettersMap.size(); 
					keyKeys = keyGettersMap.keySet().toArray(new String[0]);
					break;
				case TYPE_VALUES:
					valueGettersMap = ReflectionUtil.getAllGettersMap(value.getClass());
					valueGettersMap = getGettersMap(valueGettersMap, columnList);
					columnCount = valueGettersMap.size();
					valueKeys = valueGettersMap.keySet().toArray(new String[0]);
					break;
				case TYPE_KEYS_VALUES:
					keyGettersMap = ReflectionUtil.getAllGettersMap(key.getClass());
					keyGettersMap = getGettersMap(keyGettersMap, columnList);
					valueGettersMap = ReflectionUtil.getAllGettersMap(value.getClass());
					valueGettersMap = getGettersMap(valueGettersMap, columnList);
					columnCount = keyGettersMap.size() + valueGettersMap.size(); 
					keyKeys = keyGettersMap.keySet().toArray(new String[0]);
					valueKeys = valueGettersMap.keySet().toArray(new String[0]);
				default:
					break;
				}
				
				Set<String> keySet = valueGettersMap.keySet();
				// take care of case senstiveness
				if (primaryColumn != null) {
					for (String keyStr : keySet) {
						if (primaryColumn.equalsIgnoreCase(keyStr)) {
							primaryColumn = keyStr;
							break;
						}
					}
				}
				
				// Remove all duplicate entries from keyGettersMap
				// keep only primary key (column)
				for (String keyStr : keySet) {
					if (isKeyPrimary == false || primaryColumn == null || primaryColumn.equalsIgnoreCase(keyStr) == false) {
						keyGettersMap.remove(keyStr);
					}
				}
				break;
			}
			
			try {
				
				String insertQuery = createInsertQuery(keyKeys, valueKeys, tableName);
				String updateQuery = createUpdateQuery(keyKeys, valueKeys, tableName, primaryColumn, isKeyPrimary);
//				
//				System.out.println(insertQuery);
//				System.out.println(updateQuery);
				
				// tries insert followed by update (upsert)
				for (Region.Entry entry : entrySet) {
					Object key = entry.getKey();
					Object value = entry.getValue();
					
					// try insert first. if it fails then do update
			    	PreparedStatement statement = createInsertPreparedStatement(keyGettersMap, valueGettersMap, keyKeys, valueKeys, key, value, insertQuery);
//			    	System.out.println(statement.toString());
			    	try {
			    		statement.executeUpdate();
			    	} catch (SQLException ex) {
			    		
			    		// try update
			    		statement = createUpdatePreparedStatement(keyGettersMap, valueGettersMap, keyKeys, valueKeys, key, value, updateQuery, primaryColumn, isKeyPrimary);
			    		statement.executeUpdate();
//			    		System.out.println(statement.toString());
			    	}
			    	count++;
				}
	    	} catch (SQLException ex) {
	    		throw new DBUtilException(ex);
	    	}
		
		} catch (Exception ex) {
			throw new DBUtilException(ex);
		}
		return count;
	}
	
	private List<ColumnInfo> getColumnInfoList(String tableName) throws SQLException
	{
		String lowercase = tableName.toLowerCase();
		ArrayList<ColumnInfo> columnList = new ArrayList();
		if (lowercase.startsWith("insert ")) {
			// insert
			
			
		} else if (lowercase.startsWith("update ")) {
			
			// update
			
			// TODO: hack
			// Replace the following with real parser
			// This breaks if string literals that has '?', ',', or '=' character
			String split[] = tableName.split("= *\\?");
			for (int i = 0; i < split.length; i++) {
				String token = split[i].trim();
				int index = token.indexOf('=');
				if (index >= 0) {
					String x = token.substring(index);
					x = x.trim();
					index = x.lastIndexOf(',');
					if (index > 0) {
						token = x.substring(index+1);
					} else {
						index = x.lastIndexOf(' ');
						if (index > 0) {
							token = x.substring(index+1);
						} else {
							continue;
						}
					}
				}
				index = token.lastIndexOf(' ');
				if (index != -1) {
					token = token.substring(index);
				}
				token = token.replace(',', ' ');
				token = token.trim();
				ColumnInfo info = new ColumnInfo();
				info.name = token;
				columnList.add(info);
			}
			
		} else {
			// table name
			DatabaseMetaData meta = conn.getMetaData();
			ResultSet resultSet = meta.getColumns(null, null, tableName, null);
			
			while (resultSet.next()) {
				ColumnInfo info = new ColumnInfo();
				info.name = (String)resultSet.getObject("COLUMN_NAME");
				info.type = (Integer)resultSet.getObject("DATA_TYPE"); // java.sql.Types
				columnList.add(info);
			}
		}
		
		return columnList;
	}
	
	private String[] getColumnNames(List<ColumnInfo> columnList)
	{
		String columnNames[] = new String[columnList.size()];
		for (int i = 0; i < columnList.size(); i++) {
			columnNames[i] = ((ColumnInfo)columnList.get(i)).name;
		} 
		return columnNames;
	}
	
	private int storeMapDB(Gfsh gfsh, Map map, String tableName, 
			int storeType, String primaryColumn, 
			boolean isKeyPrimary, List<ColumnInfo> columnList, boolean isQuery) throws DBUtilException
	{
		int count = 0;
		try {
			
			// Collect all matching field names from the region object
			Set<Map.Entry> entrySet = map.entrySet();
			
			Map<String, Method> keyGettersMap = new HashMap(0);
			Map<String, Method> valueGettersMap = new HashMap(0);
			int columnCount = 0; 
			String keyKeys[] = new String[0];
			String valueKeys[] = new String[0];
			for (Map.Entry entry : entrySet) {
				Object key = entry.getKey();
				Object value = entry.getValue();
				switch (storeType) {
				case TYPE_KEYS:
					keyGettersMap = ReflectionUtil.getAllGettersMap(key.getClass());
					keyGettersMap = getGettersMap(keyGettersMap, columnList);
					columnCount = keyGettersMap.size(); 
					if (isQuery) {
						keyKeys = getColumnNames(columnList);
					} else { 
						keyKeys = keyGettersMap.keySet().toArray(new String[0]);
					}
					
					break;
				case TYPE_VALUES:
					valueGettersMap = ReflectionUtil.getAllGettersMap(value.getClass());
					valueGettersMap = getGettersMap(valueGettersMap, columnList);
					columnCount = valueGettersMap.size();
					if (isQuery) {
						valueKeys = getColumnNames(columnList);
					} else { 
						valueKeys = valueGettersMap.keySet().toArray(new String[0]);
					}
					break;
				case TYPE_KEYS_VALUES:
					keyGettersMap = ReflectionUtil.getAllGettersMap(key.getClass());
					keyGettersMap = getGettersMap(keyGettersMap, columnList);
					valueGettersMap = ReflectionUtil.getAllGettersMap(value.getClass());
					valueGettersMap = getGettersMap(valueGettersMap, columnList);
					columnCount = keyGettersMap.size() + valueGettersMap.size(); 
					
					if (isQuery) {
						keyKeys = getColumnNames(columnList);
						valueKeys = getColumnNames(columnList);
					} else {
						keyKeys = keyGettersMap.keySet().toArray(new String[0]);
						valueKeys = valueGettersMap.keySet().toArray(new String[0]);
					}
				default:
					break;
				}
				
				Set<String> keySet = valueGettersMap.keySet();
				// take care of case senstiveness
				if (primaryColumn != null) {
					for (String keyStr : keySet) {
						if (primaryColumn.equalsIgnoreCase(keyStr)) {
							primaryColumn = keyStr;
							break;
						}
					}
				}
				
				// Remove all duplicate entries from keyGettersMap
				// keep only primary key (column)
				for (String keyStr : keySet) {
					if (isKeyPrimary == false || primaryColumn == null || primaryColumn.equalsIgnoreCase(keyStr) == false) {
						keyGettersMap.remove(keyStr);
					}
				}
				break;
			}
			
			try {
				
				String insertQuery = createInsertQuery(keyKeys, valueKeys, tableName);
				String updateQuery = createUpdateQuery(keyKeys, valueKeys, tableName, primaryColumn, isKeyPrimary);
				if (gfsh.isDebug()) {
					System.out.println("insert: " + insertQuery);
					System.out.println("update: " + updateQuery);
				}

				// tries insert followed by update (upsert)
				for (Map.Entry entry : entrySet) {
					Object key = entry.getKey();
					Object value = entry.getValue();
					
					// try insert first. if it fails then do update
			    	PreparedStatement statement;
			    	if (insertQuery != null) {
			    		try {
			    			statement = createInsertPreparedStatement(keyGettersMap, valueGettersMap, keyKeys, valueKeys, key, value, insertQuery);
				    		statement.executeUpdate();
				    	} catch (SQLException ex) {
				    		
				    		// try update
				    		if (updateQuery != null) {
					    		statement = createUpdatePreparedStatement(keyGettersMap, valueGettersMap, keyKeys, valueKeys, key, value, updateQuery, primaryColumn, isKeyPrimary);
					    		statement.executeUpdate();
				    		}

				    	}
			    	} else {
			    		statement = createUpdatePreparedStatement(keyGettersMap, valueGettersMap, keyKeys, valueKeys, key, value, updateQuery, primaryColumn, isKeyPrimary);
			    		statement.executeUpdate();
			    	}
			    	
			    	count++;
				}
	    	} catch (SQLException ex) {
	    		throw new DBUtilException(ex);
	    	}
		
		} catch (Exception ex) {
			throw new DBUtilException(ex);
		}
		return count;
	}
	
	public int storeDB(Gfsh gfsh, String oql, String tableName, 
			int storeType, String primaryColumn, 
			boolean isKeyPrimary, int batchSize) throws DBUtilException
	{
		String lowercase = tableName.toLowerCase();
		boolean isQuery = lowercase.startsWith("insert ") || lowercase.startsWith("update ");
		int count = 0;
		int actualSize;
		String insertQuery = null;
		String updateQuery = null;
		int columnCount = 0; 
		Map<String, Method> valueGettersMap = null;
		String valueKeys[] = null;
		try {
			
			List<ColumnInfo>columnList = getColumnInfoList(tableName);
			
			// Use command client to fetch data from the server
			do {
				CommandResults cr = gfsh.getCommandClient().execute(new QueryTask(oql, batchSize, true));
				if (cr.getCode() == QueryTask.ERROR_QUERY) {
					gfsh.println(cr.getCodeMessage());
					return -1;
				}
				QueryResults results = (QueryResults) cr.getDataObject();
				if (results == null || results.getResults() == null) {
					return count;
				}
				
				if (results.getResults() instanceof Map) {
					
					// Map
					
					Map map = (Map)results.getResults();
					count += storeMapDB(gfsh, map, tableName, storeType, primaryColumn, isKeyPrimary, columnList, isQuery);
					
				} else {

					// SelectResults
					
					SelectResults sr = (SelectResults)results.getResults();
					CollectionType type = sr.getCollectionType();
					ObjectType elementType = type.getElementType();
					List srList = sr.asList();
					Object element = null;
					StructType structType = null;
					Struct struct = null;
					
					// Create insert prepared query strings
					if (insertQuery == null && srList.size() > 0) {
						element = srList.get(0);
						
						if (elementType.isStructType()) {
							// struct
							structType = (StructType) elementType;
							insertQuery = createInsertQuery(structType.getFieldNames(), tableName);
							updateQuery = createUpdateQuery(structType.getFieldNames(), tableName, primaryColumn);
						} else {
							// object
							valueGettersMap = new HashMap(0);
							valueGettersMap = ReflectionUtil.getAllGettersMap(element.getClass());
							valueGettersMap = getGettersMap(valueGettersMap, columnList);
							columnCount = valueGettersMap.size();
							
							if (isQuery) {
								valueKeys = getColumnNames(columnList);
							} else {
								valueKeys = valueGettersMap.keySet().toArray(new String[0]);
							}
							
							insertQuery = createInsertQuery(valueKeys, tableName);
							updateQuery = createUpdateQuery(valueKeys, tableName, primaryColumn);
						}
						
						if (gfsh.isDebug()) {
							System.out.println("insert: " + insertQuery);
							System.out.println("update: " + updateQuery);
						}
					}
						
					// Upsert
					for (int i = 0; i < srList.size(); i++) {
						element = srList.get(i);
	
						if (elementType.isStructType()) {
							
							structType = (StructType) elementType;
							struct = (Struct) element;
							PreparedStatement statement = createInsertPreparedStatement(struct, insertQuery);
//							System.out.println(statement.toString());
							
							try {
					    		statement.executeUpdate();
					    	} catch (SQLException ex) {
					    		
					    		// try update
					    		statement = createUpdatePreparedStatement(struct, updateQuery, primaryColumn);
					    		statement.executeUpdate();
//					    		System.out.println(statement.toString());
					    	}
	
						} else {
	
							PreparedStatement statement;
							if (insertQuery != null) {
								try {
									statement = createInsertPreparedStatement(valueGettersMap, valueKeys, element, insertQuery);
						    		statement.executeUpdate();
						    	} catch (SQLException ex) {
						    		
						    		// try update
						    		if (updateQuery != null) {
							    		statement = createUpdatePreparedStatement(valueGettersMap, valueKeys, element, updateQuery, primaryColumn);
							    		statement.executeUpdate();
						    		}
						    	}
							} else {
								statement = createUpdatePreparedStatement(valueGettersMap, valueKeys, element, updateQuery, primaryColumn);
								System.out.println(statement.toString());
					    		statement.executeUpdate();
							}
						}
					}
					
					count += sr.size();
				}
				actualSize = results.getActualSize();
				oql = null;
			} while (count < actualSize);
			
		} catch (Exception ex) {
			throw new DBUtilException(ex);
		}
		return count;
	}
	
	private PreparedStatement createUpdatePreparedStatement(
			Struct struct,
			String query,
			String primaryColumn) throws SQLException, InvocationTargetException, IllegalAccessException
	{
		Object primaryObj = null;
		PreparedStatement statement = conn.prepareStatement(query);
		String fieldNames[] = struct.getStructType().getFieldNames();
		Object fieldValues[] = struct.getFieldValues();
		int i;
    	for (i = 0; i < fieldValues.length; i++) {
    		statement.setObject(i+1, fieldValues[i]);
    		Object obj = fieldValues[i];
    		if (primaryColumn != null && primaryColumn.equals(fieldNames[i])) {
    			primaryObj = obj;
    		} else {
    			statement.setObject(i+1, obj);
    		}
    	}
    	
    	if (primaryObj != null) {
    		statement.setObject(i+1, primaryObj);
    	}
    	
    	return statement;
	}
	
	private PreparedStatement createInsertPreparedStatement(Struct struct, String query) 
				throws SQLException, InvocationTargetException, IllegalAccessException
	{
		PreparedStatement statement = conn.prepareStatement(query);
		Object fieldValues[] = struct.getFieldValues();
    	for (int i = 0; i < fieldValues.length; i++) {
    		statement.setObject(i+1, fieldValues[i]);
		}
    	return statement;
	}
	
	
	private PreparedStatement createUpdatePreparedStatement( 
			Map<String, Method> valueGettersMap, 
			String valueKeys[],
			Object value,
			String query,
			String primaryColumn) throws SQLException, InvocationTargetException, IllegalAccessException
	{
		return createUpdatePreparedStatement(null, valueGettersMap, null, valueKeys, null, value, query, primaryColumn, false);
	}
	
	private PreparedStatement createInsertPreparedStatement(
			Map<String, Method> valueGettersMap, 
			String valueKeys[],
			Object value,
			String query) throws SQLException, InvocationTargetException, IllegalAccessException
	{
		return createInsertPreparedStatement(null, valueGettersMap, null, valueKeys, null, value, query);
	}
	
	private PreparedStatement createUpdatePreparedStatement(Map<String, Method> keyGettersMap, 
			Map<String, Method> valueGettersMap, 
			String keyKeys[], 
			String valueKeys[],
			Object key,
			Object value,
			String query,
			String primaryColumn,
			boolean isKeyPrimary) throws SQLException, InvocationTargetException, IllegalAccessException
	{
		Object primaryObj = null;
		PreparedStatement statement = conn.prepareStatement(query);
		int index = 1;
		if (keyKeys != null) {
	    	for (int i = 0; i < keyKeys.length; i++) {
	    		Method method = keyGettersMap.get(keyKeys[i]);
	    		Object obj = method.invoke(key, (Object[])null);
	    		if (primaryColumn != null && isKeyPrimary==false && primaryColumn.equals(keyKeys[i])) {
	    			primaryObj = obj;
	    		} else {
	    			statement.setObject(index++, obj);
	    		}
			}
		}
		
		if (valueKeys != null) {
	    	for (int i = 0; i < valueKeys.length; i++) {
	    		Method method = valueGettersMap.get(valueKeys[i]);
	    		Object obj = method.invoke(value, (Object[])null);
	    		if (primaryColumn != null && isKeyPrimary == false && primaryColumn.equals(valueKeys[i])) {
	    			primaryObj = obj;
	    		} else {
	    			statement.setObject(index++, obj);
	    		}
			}
		}
    	
    	if (primaryObj != null) {
    		statement.setObject(index, primaryObj);
    	}
    	
    	return statement;
	}
	
	private PreparedStatement createInsertPreparedStatement(Map<String, Method> keyGettersMap, 
			Map<String, Method> valueGettersMap, 
			String keyKeys[], 
			String valueKeys[],
			Object key,
			Object value,
			String query) throws SQLException, InvocationTargetException, IllegalAccessException
	{
		PreparedStatement statement = conn.prepareStatement(query);
		if (keyKeys!= null) {
	    	for (int i = 0; i < keyKeys.length; i++) {
	    		Method method = keyGettersMap.get(keyKeys[i]);
	    		Object obj = method.invoke(key, (Object[])null);
	    		statement.setObject(i+1, obj);
			}
		}
		if (valueKeys != null) {
	    	for (int i = 0; i < valueKeys.length; i++) {
	    		Method method = valueGettersMap.get(valueKeys[i]);
	    		Object obj = method.invoke(value, (Object[])null);
	    		statement.setObject(i+1, obj);
			}
		}
    	
    	return statement;
	}
	
	private String createUpdateQuery(String fieldNames[],
			String tableName, 
			String primaryColumn)
	{
		String query = null;
		String lowercase = tableName.trim().toLowerCase();
		if (lowercase.startsWith("insert ")) {
			// insert not honored for update operation
			query = null;
		} else if (lowercase.startsWith("update ")) {
			// use the passed in update statement
			query = tableName;
		} else {
			
			// build update
			query = "UPDATE " + tableName + " SET ";
			for (int i = 0; i < fieldNames.length; i++) {
				if (primaryColumn != null && primaryColumn.equals(fieldNames[i])) {
					// skip
				} else {
					query += fieldNames[i] + "=?,";
				}
			}
		
			int index = query.lastIndexOf(",");
			if (index != -1) {
				query = query.substring(0, index);
			}
			if (primaryColumn != null) {
				query += " WHERE " + primaryColumn + "=?";
			}
		}
		
		return query;
	}
	
	private String createInsertQuery(String fieldNames[], String tableName)
	{
		
		String lowercase = tableName.trim().toLowerCase();
		String query = null;
		
		if (lowercase.startsWith("insert ")) {
			// use the passed in insert statement
			query = tableName;
		} else if (lowercase.startsWith("update ")) {
			// update not honored for insert operation
			query = null;
		} else {

			// build insert
			query = "INSERT INTO " + tableName + " (";
			for (int i = 0; i < fieldNames.length; i++) {
				query += fieldNames[i] + ",";
			}
			int index = query.lastIndexOf(',');
			if (index != -1) {
				query = query.substring(0, index);
			}
			int columnCount = fieldNames.length;  
			query += ") VALUES (";
			for (int i = 0; i < columnCount; i++) {
				query += "?,";
			}
			index = query.lastIndexOf(',');
			if (index != -1) {
				query = query.substring(0, index);
			}
			query += ")";
		}
		
		return query;
	}
	
	private String createUpdateQuery(String keyKeys[], String valueKeys[], 
			String tableName, 
			String primaryColumn,
			boolean isKeyPrimary)
	{
		String query = null;
		String lowercase = tableName.trim().toLowerCase();
		if (lowercase.startsWith("insert ")) {
			// insert not honored for update operation
			query = null;
		} else if (lowercase.startsWith("update ")) {
			// use the passed in update statement
			query = tableName;
		} else {
			
			// build update
			query = "UPDATE " + tableName + " SET ";
			for (int i = 0; i < keyKeys.length; i++) {
				if (primaryColumn != null && isKeyPrimary && primaryColumn.equals(keyKeys[i])) {
					// skip
				} else {
					query += keyKeys[i] + "=?,";
				}
			}
			for (int i = 0; i < valueKeys.length; i++) {
				if (primaryColumn != null && isKeyPrimary == false && primaryColumn.equals(valueKeys[i])) {
					// skip
				} else {
					query += valueKeys[i] + "=?,";
				}
			}
			int index = query.lastIndexOf(",");
			if (index != -1) {
				query = query.substring(0, index);
			}
			if (primaryColumn != null) {
				query += " WHERE " + primaryColumn + "=?";
			}
		}
		
		return query;
	}
	
	private String createInsertQuery(String keyKeys[], String valueKeys[], String tableName)
	{
		String lowercase = tableName.trim().toLowerCase();
		String query = null;
		
		if (lowercase.startsWith("insert ")) {
			// use the passed in insert statement
			query = tableName;
		} else if (lowercase.startsWith("update ")) {
			// update not honored for insert operation
			query = null;
		} else {
		
			// build insert
			int columnCount = 0;
			query = "INSERT INTO " + tableName + " (";
			if (keyKeys != null) {
				for (int i = 0; i < keyKeys.length; i++) {
					query += keyKeys[i] + ",";
				}
				columnCount += keyKeys.length;
			}
			if (valueKeys != null) {
				for (int i = 0; i < valueKeys.length; i++) {
					query += valueKeys[i] + ",";
				}
				columnCount += valueKeys.length;
			}
			int index = query.lastIndexOf(',');
			if (index != -1) {
				query = query.substring(0, index);
			}
			
			query += ") VALUES (";
			for (int i = 0; i < columnCount; i++) {
				query += "?,";
			}
			index = query.lastIndexOf(',');
			if (index != -1) {
				query = query.substring(0, index);
			}
			query += ")";
		}
		
		return query;
	}
	
	private Map getGettersMap(Map<String, Method> gettersMap, List<ColumnInfo> columnList)
	{
		HashMap map = new HashMap();
		Set<String> keySet = gettersMap.keySet();
		for (int i = 0; i < columnList.size(); i++) {
			ColumnInfo info = columnList.get(i);
			Method getterMethod  = gettersMap.get("get" + info.name);
			if (getterMethod == null) {
				for (String key : keySet) {
					if (key.substring(3).equalsIgnoreCase(info.name)) {
						getterMethod = gettersMap.get(key);
						break;
					}
				}
			}
			if (getterMethod != null) {
				map.put(info.name, getterMethod);
			}
		}
		return map;
	}
	
	private Object updateObject(Gfsh gfsh, Class clazz, 
			Map<String, Method> settersMap, Map<String, Method> realMap,
			ResultSet resultSet,
			String primaryColumn) throws Exception
	{
		
		// If primaryColumn is set then set the mapping method only
		
		if (primaryColumn != null) {
			
			// Set the primary column value only
			Object obj = resultSet.getObject(primaryColumn);
			if (clazz == null ||
				clazz == byte.class || clazz == Byte.class ||
				clazz == char.class || clazz == Character.class ||
				clazz == short.class || clazz == Short.class ||
				clazz == int.class || clazz == Integer.class ||
				clazz == long.class || clazz == Long.class ||
				clazz == float.class || clazz == Float.class ||
				clazz == double.class || clazz == Double.class ||
				clazz == Date.class || clazz == String.class)
			{
				return obj;
			}
			
			Object value = clazz.newInstance();    		
    		Method setterMethod  = realMap.get(primaryColumn);
			if (setterMethod == null) {
				setterMethod = settersMap.get("set" + primaryColumn);
				if (setterMethod == null) {
					Set<Entry<String, Method>> entrySet = settersMap.entrySet(); //FindBugs - entrySet efficient over keyset
					for (Entry<String, Method> entry : entrySet) {
					  String key  = entry.getKey();
            if (key.substring(3).equalsIgnoreCase(primaryColumn)) {
            setterMethod = entry.getValue();
            realMap.put(primaryColumn, settersMap.get(key));
            }
          }
				}
			}
			setterMethod.invoke(value, obj);
    		return value;
		}
		
		// Set the entire object
		
		if (clazz == null ||
			clazz == byte.class || clazz == Byte.class ||
			clazz == char.class || clazz == Character.class ||
			clazz == short.class || clazz == Short.class ||
			clazz == int.class || clazz == Integer.class ||
			clazz == long.class || clazz == Long.class ||
			clazz == float.class || clazz == Float.class ||
			clazz == double.class || clazz == Double.class ||
			clazz == Date.class || clazz == String.class)
		{
			
			return resultSet.getObject(1);
			
		} else {
			
			Object value = clazz.newInstance();
    		ResultSetMetaData meta = resultSet.getMetaData();
    		for (int i = 1; i <= meta.getColumnCount(); i++) {
    			String columnName = meta.getColumnName(i);
    			Method setterMethod  = realMap.get(columnName);
    			if (setterMethod == null) {
    				setterMethod = settersMap.get("set" + columnName);
    				if (setterMethod == null) {
    					Set<String> keySet = settersMap.keySet();
    					for (String key : keySet) {
							if (key.substring(3).equalsIgnoreCase(columnName)) {
								setterMethod = settersMap.get(key);
								realMap.put(columnName, settersMap.get(key));
							}
						}
    				}
    			}
    			if (setterMethod == null) {
//	        				throw new DBUtilException(DBUtilException.ERROR_NO_MATCHING_METHOD_FOR_COLUMN, "Undefined method for the column " + columnName);
    				continue;
    			}
    			
    			Object obj = resultSet.getObject(i);
    			setterMethod.invoke(value, obj);
    		}
    		return value;
		}

	}
	//FindBugs - make static inner class
	static class ColumnInfo
	{
		String name;
		int type;
	}
}
