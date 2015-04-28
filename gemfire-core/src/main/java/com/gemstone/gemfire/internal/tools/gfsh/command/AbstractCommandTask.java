package com.gemstone.gemfire.internal.tools.gfsh.command;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.ExpirationAction;
import com.gemstone.gemfire.cache.ExpirationAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.internal.tools.gfsh.util.RegionUtil;

/**
 * AbstractCommandTask provides sendResults() for sending CommandResult
 * objects asynchronously.
 * @author dpark
 *
 */
public abstract class AbstractCommandTask implements CommandTask 
{
	private static final long serialVersionUID = 1L;

	private static final String KEY_RESULTS_DEFAULT = "results";
	
	private static final String REGION_NAME_ASYNC = "async";
	
	// The default of 2 minute idle time allowed
	private static int ASYNC_REGION_TIMEOUT = Integer.getInteger("AbstractCommandTask.ASYNC_REGION_TIMEOUT", 120);
	
	Region commandRegion;
	
	private String resultSetRegionFullPath;
	
	protected Region getCommandRegion()
	{
		return commandRegion;
	}
	
	public void setCommandRegion(Region commandRegion) {
	  this.commandRegion = commandRegion;
  }
	
	/**
	 * Sets the region in which the results to be sent. This is
	 * invoked by CommandServerManager. Do not invoke it directly.
	 * @param resultSetRegionFullPath
	 */
	/*protected*/public void setResultSetRegionPath(String resultSetRegionFullPath)
	{
		this.resultSetRegionFullPath = resultSetRegionFullPath;
	}
	
	/**
	 * Returns the region used to keep track of asynchronous regions. 
	 * The command client is allowed to supply an optional inbox region
	 * with which it may receive results. The server removes these inbox
	 * regions if they have idle for more that ASYNC_REGION_TIMEOUT. 
	 * @return the region used to keep track of asynchronous regions
	 */
	private Region getAsyncRegion()
	{
		Region asyncRegion = getCommandRegion().getSubregion(REGION_NAME_ASYNC);
		if (asyncRegion == null) {
			AttributesFactory factory = new AttributesFactory();
			factory.setStatisticsEnabled(true);
			factory.setScope(Scope.LOCAL);
			factory.setDataPolicy(DataPolicy.NORMAL);
			factory.setEntryIdleTimeout(new ExpirationAttributes(ASYNC_REGION_TIMEOUT, ExpirationAction.LOCAL_DESTROY));
			factory.addCacheListener(new CacheListenerAdapter() {
				public void afterDestroy(EntryEvent event)
				{
					String regionPath = (String)event.getKey();
					getCommandRegion().getSubregion(regionPath).destroyRegion();
				}
			});
			try {
				asyncRegion = getCommandRegion().createSubregion(REGION_NAME_ASYNC, factory.create());
			} catch (Exception ex) {
				// in case another thread created it
				asyncRegion = getCommandRegion().getSubregion(REGION_NAME_ASYNC);
			}
		}
		return asyncRegion;
	}
	
	/**
	 * Sends the specified results to the client's private inbox region.
	 * The inbox region is a sub-region of the command region. Invoke
	 * this method if the results are too large to send synchronously.
	 * The client must supply a listener using 
	 * CommandClient.addCommandResultsListener() in order to receive
	 * the results.
	 * @param results The results to send to the client.
	 */
	protected void sendResults(CommandResults results)
	{
		try {
			Region resultSetRegion = RegionUtil.getRegion(resultSetRegionFullPath, Scope.LOCAL, DataPolicy.EMPTY, null);
			resultSetRegion.put(KEY_RESULTS_DEFAULT, results);
			Region asyncRegion = getAsyncRegion();
			asyncRegion.put(resultSetRegion.getName(), true);
		} catch (CacheException ex) {
			Cache cache = CacheFactory.getAnyInstance();
			cache.getLogger().error(ex);
		}
	}

	public void fromData(DataInput input) throws IOException, ClassNotFoundException
	{
		resultSetRegionFullPath = DataSerializer.readString(input);
	}

	public void toData(DataOutput output) throws IOException
	{
		DataSerializer.writeString(resultSetRegionFullPath, output);
	}
	
	
	/**
	 * Runs this task. A client executes CommandTak by calling
	 * CommandClient.execute(CommandTask task).
	 * @param userData The userData optionally provided by the cache server. The
	 *                 cache server may pass any user data to the command task.
	 * @return Returns the task results.
	 */
	public abstract CommandResults runTask(Object userData);

}
