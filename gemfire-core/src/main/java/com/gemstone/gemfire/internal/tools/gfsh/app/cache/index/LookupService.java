package com.gemstone.gemfire.internal.tools.gfsh.app.cache.index;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.internal.util.BlobHelper;
import com.gemstone.gemfire.internal.tools.gfsh.app.cache.index.task.IndexInfoTask;
import com.gemstone.gemfire.internal.tools.gfsh.app.cache.index.task.QuerySizeTask;
import com.gemstone.gemfire.internal.tools.gfsh.app.cache.index.task.QueryTask;
import com.gemstone.gemfire.internal.tools.gfsh.app.command.CommandClient;
import com.gemstone.gemfire.internal.tools.gfsh.command.CommandResults;

/**
 * LookupService provides custom query access to GemFire cache. It supports 
 * equality and "AND" conditions only. The server must install 
 * com.gemstone.gemfire.internal.tools.gfsh.cache.index.Indexer before LookupService can
 * be used. The gfcommand addon library includes the sample implementation 
 * com.gemstone.gemfire.tools.gfsh.cache.index.IndexBuilder which is for demo
 * purposes only as it lacks HA and recovery support.
 * @author dpark
 */
public class LookupService
{
	CommandClient commandClient;

	public LookupService(String poolNameOrEndpoints)
	{
		this(null, poolNameOrEndpoints);
	}
	
	/**
	 * Constructs a new LookupService object.
	 * @param commandRegionPath The command region full path. This region is 
	 *  						used to send query commands to the cache. If
	 *  		null, the default "/__command" region is used.
	 * @param poolNameOrEndpoints A pool name or comma separated list of GemFire cache servers.
	 *            The endpoints format is "e1=host:port,e2=host2:port".
	 *            If the name is not in the endpoints format then it is treated
	 *            as a pool name.
	 * @exception Throws a LookupServiceException if it encounters an error from the 
     *            underlying GemFire communications mechanism.
	 */
	public LookupService(String commandRegionPath, String poolNameOrEndpoints)
	{
		try {
			commandClient = new CommandClient(commandRegionPath, poolNameOrEndpoints);
		} catch (Exception ex) {
			throw new LookupServiceException("Unable to create LookupService due to a cache exception: " + ex.getMessage(), ex);
		}
	}
	
	/**
	 * Constructs a new LookupService object with the specified command client 
	 * object.
	 * @param commandClient The command client object.
	 */
	public LookupService(CommandClient commandClient)
	{
		this.commandClient = commandClient;
	}
	
	/**
	 * Returns the command region path. It returns null, if the command
	 * client is not defined.
	 */
	public String getCommandRegionPath()
	{
		if (commandClient == null) {
			return null;
		}
		return commandClient.getOutboxRegionFullPath();
	}
	
	/**
	 * Returns the endpoints. It returns null if a pool is in use.
	 */
	public String getEndpoints()
	{
		if (commandClient == null) {
			return null;
		}
		return commandClient.getEndpoints();
	}
	
	/**
	 * Returns the pool name. It returns null if endpoints are used.
	 */
	public String getPoolName()
	{
		if (commandClient == null) {
			return null;
		}
		return commandClient.getPoolName();
	}
	
	/**
	 * Returns the query results in the form of Map. The returned
     * Map contains the specified region (key, value) pairs.
     * 
	 * @param regionPath The full path of a region to query. Note that this region path is
     * not the same as the command region path specified in the LookupService constructor. This
     * region contains data that the query is to be executed. The LookupService constructor's 
     * region path is used for sending the query command.
     * 
	 * @param queryKey The query key object that contains the fields to search.
	 * @return Returns the query results in the form of Map. The returned
     * 			Map contains the specified region (key, value) pairs.
	 * @exception Throws a LookupServiceException if it encounters an error from the 
     *            underlying GemFire communications mechanism.
	 */
	public Map entryMap(String regionPath, Object queryKey)
	{
		try {
			CommandResults results = commandClient.execute(new QueryTask(regionPath, queryKey, QueryTask.TYPE_KEYS_VALUES));
			Map resultMap = null;
			switch (results.getCode()) {
			case QueryTask.SUCCESS_RR:
				{
					byte[] blob = (byte[])results.getDataObject();
					if (blob != null) {
						resultMap = (Map)BlobHelper.deserializeBlob(blob);
					}
				}
				break;
				
			case QueryTask.SUCCESS_PR:
				{
					List list = (List)results.getDataObject();
					try {
						Iterator<byte[]> iterator = list.iterator();
						while (iterator.hasNext()) {
							byte[] blob = iterator.next();
							if (blob != null) {
								Map map = (Map)BlobHelper.deserializeBlob(blob);
								if (resultMap == null) {
									resultMap = map;
								} else {
									resultMap.putAll(map);
								}
							}
						}
					} catch (Exception ex) {
						CacheFactory.getAnyInstance().getLogger().warning("Error occurred while deserializing to blob: " + ex.getMessage(), ex);
					}
				}
				break;
			}
			
			return resultMap;
		} catch (Exception ex) {
			throw new LookupServiceException("Unable to retrieve the entry map due to a cache exception: " + ex.getMessage(), ex);
		}
	}
	
	/**
	 * Returns the query results in the form of Set. The returned
     * Set contains the specified region keys.
	 * @param regionPath The full path of a region to query. Note that this region path is
     * not the same as the command region path specified in the LookupService constructor. This
     * region contains data that the query is to be executed. The LookupService constructor's 
     * region path is used for sending the query command.
	 * @param queryKey The query key object that contains the fields to search.
	 * @return Returns the query results in the form of Set. The returned
     * 			Set contains the specified region keys.
	 */
	public Set keySet(String regionPath, Object queryKey)
	{
		try {
			CommandResults results = commandClient.execute(new QueryTask(regionPath, queryKey, QueryTask.TYPE_KEYS));
			Set keys = null;
			switch (results.getCode()) 
			{
				case QueryTask.SUCCESS_RR: 
					{
						byte[] blob = (byte[]) results.getDataObject();
		
						if (blob != null) {
							keys = (Set) BlobHelper.deserializeBlob(blob);
						}
					}
					break;
					
				case QueryTask.SUCCESS_PR: 
					{
						try {
							List list = (List)results.getDataObject();
							Iterator<byte[]> iterator = list.iterator();
							while (iterator.hasNext()) {
								byte[] blob = iterator.next();
								if (blob != null) {
									Set set = (Set)BlobHelper.deserializeBlob(blob);
									if (keys == null) {
										keys = set;
									} else {
										keys.addAll(set);
									}
								}
							}
						} catch (Exception ex) {
							CacheFactory.getAnyInstance().getLogger().warning("Error occurred while deserializing to blob: " + ex.getMessage(), ex);
						}
					}
					break;
			}
			return keys;
		} catch (Exception ex) {
			throw new LookupServiceException("Unable to retrieve the key set due to a cache exception: " + ex.getMessage(), ex);
		}
	}
	
	/**
	 * Returns the query results in the form of Collection. The returned
     * Collection contains the specified region values.
	 * @param regionPath The full path of a region to query. Note that this region path is
     * not the same as the command region path specified in the LookupService constructor. This
     * region contains data that the query is to be executed. The LookupService constructor's 
     * region path is used for sending the query command.
	 * @param queryKey The query key object that contains the fields to search.
	 * @return Returns the query results in the form of Collection. The returned
     * 			Collection contains the specified region values.
	 */
	public Collection values(String regionPath, Object queryKey)
	{
		try {
			CommandResults results = commandClient.execute(new QueryTask(regionPath, queryKey, QueryTask.TYPE_VALUES));
			Collection values = null;
			switch (results.getCode()) 
			{
				case QueryTask.SUCCESS_RR: 
					{
						byte[] blob = (byte[]) results.getDataObject();
		
						if (blob != null) {
							values = (Collection) BlobHelper.deserializeBlob(blob);
						}
					}
					break;
					
				case QueryTask.SUCCESS_PR: 
					{
						try {
							List list = (List)results.getDataObject();
							Iterator<byte[]> iterator = list.iterator();
							while (iterator.hasNext()) {
								byte[] blob = iterator.next();
								if (blob != null) {
									Collection col = (Collection)BlobHelper.deserializeBlob(blob);
									if (values == null) {
										values = col;
									} else {
										values.addAll(col);
									}
								}
							}
						} catch (Exception ex) {
							CacheFactory.getAnyInstance().getLogger().warning("Error occurred while deserializing to blob: " + ex.getMessage(), ex);
						}
					}
					break;
			}
			return values;
		} catch (Exception ex) {
			throw new LookupServiceException("Unable to retrieve the values to a cache exception: " + ex.getMessage(), ex);
		}
	}

    /** 
     * Returns the IndexInfo that contains the cache indexer information.
     * 
     * @param regionPath The full path of a region to query.
     * @return Returns the IndexInfo that contains the cache indexer information.
     * @exception Throws a LookupServiceException if it encounters an error from the 
     *            underlying GemFire communications mechanism.
     */
    public IndexInfo[] getIndexInfoArray(String regionPath)
    {
        try {
        	IndexInfo[] indexInfoArray = null;
        	CommandResults results = commandClient.execute(new IndexInfoTask(regionPath));
        	switch (results.getCode()) {
			case QueryTask.SUCCESS_RR:
				{
					IndexInfo indexInfo = (IndexInfo)results.getDataObject();
					if (indexInfo != null) {
						indexInfoArray = new IndexInfo[] { indexInfo };
					}
				}
				break;
			
			case QueryTask.SUCCESS_PR:
				{ 
		            List list = (List)results.getDataObject();
		            if (list != null) {
		            	indexInfoArray = (IndexInfo[])list.toArray(new IndexInfo[0]);
		            }
				}
				break;
        	}
            return indexInfoArray;
            
        } catch (Exception ex) {
            throw new LookupServiceException("Unable to retrieve index info due to a cache exception: " + ex.getMessage(), ex);
        }
    }

    /** 
     * Returns the size of the query results.
  	 *
  	 * @param regionPath The full path of a region to query.
  	 * @param queryKey The query key object that contains the fields to search.
     
     * @return Returns the size of the query results.
     * @exception Throws a LookupServiceException if it encounters an error from the 
     *            underlying GemFire communications mechanism.
     */
    public int size(String regionPath, Object queryKey)
    {
        try {
            CommandResults results = commandClient.execute(new QuerySizeTask(regionPath, queryKey));
		    return (Integer)results.getDataObject();
        }
        catch (Exception ex)
        {
            throw new LookupServiceException("Unable to retrieve the size due to a cache exception: " + ex.getMessage(), ex);
        }
    }

    /** 
     * Closes the LookupService. Note that if there is another instance of LookupService that
     * was created with the same constructor arguments, i.e., commandRegionPath and endpoints, 
     * that instance will also be closed. Invoking this method more than once will throw
     * a LookupServiceException.
     * 
     * @exception Throws a LookupServiceException if it encounters an error from the 
     *            underlying GemFire communications mechanism.
     */
    public void close()
    {
        try
        {
            commandClient.close();
        }
        catch (Exception ex)
        {
            throw new LookupServiceException("Exception raised while closing LookupService: " + ex.getMessage(), ex);
        }
    }
}
