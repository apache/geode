package com.gemstone.gemfire.internal.tools.gfsh.app.command;

import java.util.ArrayList;
import java.util.UUID;

import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.internal.tools.gfsh.app.command.task.RegionCreateTask;
import com.gemstone.gemfire.internal.tools.gfsh.app.command.task.RegionDestroyTask;
import com.gemstone.gemfire.internal.tools.gfsh.app.command.task.data.RegionAttributeInfo;
import com.gemstone.gemfire.internal.tools.gfsh.command.AbstractCommandTask;
import com.gemstone.gemfire.internal.tools.gfsh.command.CommandResults;
import com.gemstone.gemfire.internal.tools.gfsh.command.CommandTask;
import com.gemstone.gemfire.internal.tools.gfsh.util.RegionUtil;

/**
 * CommandClient executes a CommandTask in the server.
 * 
 * @author dpark
 * 
 */
public class CommandClient
{
	private static final String KEY_DEFAULT = "_command";
	private static final String KEY_BROADCAST = "_bcast";

	private Region outboxRegion;
	private String outboxRegionFullPath;
	private String endpoints;
	private Pool pool;

	private Region inboxRegion;
	private boolean inboxEnabled = false;

	private ArrayList commandResultsListenerList = new ArrayList(5);

	private CommandResultsListener commandResultsListeners[] = new CommandResultsListener[0];

	/**
	 * Constrcuts a new CommandClient object with the specified pool name or
	 * endpoints. If null, the default path "/__command" is assigned.
	 * 
	 * @param outboxRegionFullPath
	 *            The region full path.
	 * @param poolNameOrEndpoints
	 *            The bridge client pool name or endpoints.  The endpoints format 
	 *            is "e1=host:port,e2=host2:port".
	 *            If the name is not in the endpoints format then it is treated
	 *            as a pool name. If null, the region is created as
	 *            a peer region if not already created.
	 * @throws CacheException
	 *             Thrown if unable to create cache connection.
	 */
	public CommandClient(String outboxRegionFullPath, String poolNameOrEndpoints) throws CacheException
	{
	  this(outboxRegionFullPath, poolNameOrEndpoints, isPoolName(poolNameOrEndpoints));
//	  FindBugs - endpoints uninited & seems to be used by mistake, should have been poolNameOrEndpoints
//		init(outboxRegionFullPath, endpoints, isPoolName(poolNameOrEndpoints));
	}

	/**
	 * Creates a CommandClient object with the specified pool or endpoints.
	 * @param outboxRegionFullPath The outbox region path. If null, the default path "/__command" is assigned.
	 * @param poolNameOrEndpoints Pool name or endpoints.
	 * @param isPool true if pool name, false if endpoints.
	 */
    public CommandClient(String outboxRegionFullPath, String poolNameOrEndpoints, boolean isPool)
    {
        init(outboxRegionFullPath, poolNameOrEndpoints, isPool);
    }
    
    private static boolean isPoolName(String poolNameOrEndpoints)
    {
    	return poolNameOrEndpoints.indexOf(":") != -1;
    }
    
    private void init(String outboxRegionFullPath, String poolNameOrEndpoints, boolean isPool)
    {
    	if (outboxRegionFullPath == null)
        {
            this.outboxRegionFullPath = "__command";
        }
        else
        {
            this.outboxRegionFullPath = outboxRegionFullPath;
        }
        if (isPool)
        {
            pool = PoolManager.find(poolNameOrEndpoints);
            outboxRegion = RegionUtil.getRegion(this.outboxRegionFullPath, Scope.LOCAL, DataPolicy.EMPTY, pool, false);
        }
        else
        {
            endpoints = poolNameOrEndpoints;
            outboxRegion = RegionUtil.getRegion(this.outboxRegionFullPath, Scope.LOCAL, DataPolicy.EMPTY, endpoints, false);
        }
    }

	/**
	 * Constructs a new Command Client object with the specified pool.
	 * @param outboxRegionFullPath
	 * @param pool The pool.
	 * @throws CacheException Thrown if unable to create cache connection.
	 */
	public CommandClient(String outboxRegionFullPath, Pool pool) throws CacheException
	{
		if (outboxRegionFullPath == null) {
			this.outboxRegionFullPath = "__command";
		} else {
			this.outboxRegionFullPath = outboxRegionFullPath;
		}
		this.pool = pool;
		outboxRegion = RegionUtil.getRegion(this.outboxRegionFullPath, Scope.LOCAL, DataPolicy.EMPTY, pool, false);
	}

	/**
	 * Constructs a CommandClient object with the default region "__command".
	 * @param poolNameOrEndpoints
	 *            The bridge client pool name or endpoints.  The endpoints format 
	 *            is "e1=host:port,e2=host2:port".
	 *            If the name is not in the endpoints format then it is treated
	 *            as a pool name. If null, the region is created as
	 *            a peer region if not already created.
	 * @throws CacheException Thrown if unable to create cache connection.
	 */
	public CommandClient(String poolNameOrEndpoints) throws CacheException
	{
		this(null, poolNameOrEndpoints);
	}

	/**
	 * Creates a CommandClient object that uses the default command region name,
	 * "_command".
	 * 
	 * @param pool
	 */
	public CommandClient(Pool pool)
	{
		this.pool = pool;
	}

	/**
	 * Executes the specified command task.
	 * <p>
	 * The server must have CommandServerManager registered with writer disabled
	 * for this method to work.
	 * 
	 * @param task
	 *            The command task to execute in the server.
	 * @param isBroadcast
	 *            true to broadcast the command execution to all peers. false to
	 *            execute on one of the servers.
	 * @return Returns CommandResults returned by CommandTask.runTask().
	 */
	private CommandResults execute(CommandTask task, boolean isBroadcast)
	{
		if (isClosed()) {
			return null;
		}

		CommandResults results;
		if (isInboxEnabled()) {
			if (task instanceof AbstractCommandTask) {
				((AbstractCommandTask) task).setResultSetRegionPath(inboxRegion.getFullPath());
			}
		}
		if (isBroadcast) {
			return (CommandResults) outboxRegion.get(KEY_BROADCAST, task);
		} else {
			return (CommandResults) outboxRegion.get(KEY_DEFAULT, task);
		}
	}

	/**
	 * Executes the specified command task.
	 * <p>
	 * The server must have CommandServerManager registered with writer disabled
	 * for this method to work.
	 * 
	 * @param task
	 *            The command task to execute in the server.
	 * @return Returns CommandResults returned by CommandTask.runTask().
	 */
	public CommandResults execute(CommandTask task)
	{
		return execute(task, false);
	}

	/**
	 * Broadcasts the specified command task.
	 * <p>
	 * The server must have CommandServerManager registered with writer disabled
	 * for this method to work.
	 * 
	 * @param task
	 *            The command task to execute in the server.
	 * @return Returns CommandResults returned by CommandTask.runTask().
	 */
	public CommandResults broadcast(CommandTask task)
	{
		return execute(task, true);
	}

	/**
	 * Executes the task. The data object is passed to CommandTask.runTask() as
	 * user data, which has the type SerializedCacheValue. CommandTask.runTask()
	 * can inflate the serialized object to its actual type or send it to other
	 * GemFire members as a serialized object.
	 * <p>
	 * The server must have CommandServerManager registered with the writer
	 * enabled for this method to work.
	 * 
	 * @param task
	 * @param dataObject
	 */
	public void execute(CommandTask task, Object dataObject)
	{
		if (isClosed()) {
			return;
		}

		outboxRegion.put(task, dataObject);
	}

	public void setInboxEnabled(boolean inboxEnabled) throws CommandException
	{
		if (isClosed()) {
			return;
		}
		this.inboxEnabled = inboxEnabled;
		setUniqueInbox(outboxRegion.getFullPath());
	}

	public boolean isInboxEnabled()
	{
		return inboxEnabled;
	}

	/**
	 * Closes CommandClient. This object is no longer usable after this method
	 * call.
	 */
	public void close() throws CommandException
	{
		CommandResults results = null;
		if (inboxRegion != null) {
			results = broadcast(new RegionDestroyTask(inboxRegion.getFullPath()));
			inboxRegion = null;
			inboxEnabled = false;
		}

		// Destroying the outboxRegion also destroys its child region
		// inboxRegion.
		if (outboxRegion != null) {
			outboxRegion.localDestroyRegion();
			outboxRegion = null;
		}

		if (results != null && results.getCode() == RegionDestroyTask.ERROR_REGION_DESTROY) {
			throw new CommandException("Server may have not destroyed the client command region(s)", results
					.getException());
		}
	}

	public boolean isClosed()
	{
		return outboxRegion == null;
	}

	public String getInboxRegionFullPath()
	{
		if (inboxRegion == null) {
			return null;
		}
		return inboxRegion.getFullPath();
	}

	private String createUniqueRegionName() throws Exception
	{
		UUID uuid = UUID.randomUUID();
		return uuid.toString();
	}

	/**
	 * Creates a unique inbox region under the specified regionFullPath.
	 * 
	 * @param regionFullPath
	 *            The full path of the region in which a unique (and therefore
	 *            private) inbox region is created.
	 */
	private void setUniqueInbox(String regionFullPath) throws CommandException
	{
		if (regionFullPath == null) {
			inboxRegion = null;
			return;
		}

		try {
			String inboxPath = regionFullPath + "/" + createUniqueRegionName();
			RegionAttributeInfo attrInfo = new RegionAttributeInfo();
			attrInfo.setAttribute(RegionAttributeInfo.SCOPE, "local");
			attrInfo.setAttribute(RegionAttributeInfo.DATA_POLICY, "empty");
			CommandResults results = broadcast(new RegionCreateTask(inboxPath, attrInfo));

			if (results.getCode() == RegionCreateTask.ERROR_REGION_NOT_CREATED) {
				throw new CommandException("Unable to create a CommandResults reply region in the server.", results
						.getException()); //FindBugs - forgets to throw new CommandException(String, Throwable)
			}

			if (endpoints != null) {
				inboxRegion = RegionUtil.getRegion(inboxPath, Scope.LOCAL, DataPolicy.DEFAULT, endpoints, false);
			} else {
				inboxRegion = RegionUtil.getRegion(inboxPath, Scope.LOCAL, DataPolicy.DEFAULT, pool, false);
			}
			inboxRegion.getAttributesMutator().addCacheListener(new ReplyListener());
			inboxRegion.registerInterestRegex(".*");
		} catch (Exception ex) {
			throw new CommandException(ex);
		}
	}

	public SelectResults query(String queryString) throws CommandException
	{
		if (isClosed()) {
			return null;
		}

		try {
			return outboxRegion.query(queryString);
		} catch (Exception ex) {
			throw new CommandException(ex);
		}
	}

	public String getOutboxRegionFullPath()
	{
		return outboxRegionFullPath;
	}

	/**
	 * Returns the endpoints. If pool is used then it returns null.
	 */
	public String getEndpoints()
	{
		return endpoints;
	}

	/**
	 * Returns the pool. If endpoints is used then it returns null.
	 */
	public Pool getPool()
	{
		return pool;
	}
	
	/**
	 * Returns the pool name. If endpoints is used then it returns null.
	 */
	public String getPoolName()
	{
		if (pool == null) {
			return null;
		} else {
			return pool.getName();
		}
	}


	public void addCommandResultsListener(CommandResultsListener commandResultsListener)
	{
		commandResultsListenerList.add(commandResultsListener);
		commandResultsListeners = (CommandResultsListener[]) commandResultsListenerList
				.toArray(new CommandResultsListener[0]);
	}

	public void removeCommandResultsListener(CommandResultsListener commandResultsListener)
	{
		commandResultsListenerList.remove(commandResultsListener);
		commandResultsListeners = (CommandResultsListener[]) commandResultsListenerList
				.toArray(new CommandResultsListener[0]);
	}

	protected void fireCommandResults(CommandResults results)
	{
		CommandResultsListener listeners[] = commandResultsListeners;
		for (int i = 0; i < listeners.length; i++) {
			listeners[i].commandResultsReceived(results);
		}
	}

	class ReplyListener extends CacheListenerAdapter
	{
		private void handleReplyReceived(EntryEvent entryEvent)
		{
			CommandResults results = (CommandResults) entryEvent.getNewValue();
			fireCommandResults(results);
		}

		public void afterCreate(EntryEvent entryEvent)
		{
			handleReplyReceived(entryEvent);
		}

		public void afterUpdate(EntryEvent entryEvent)
		{
			handleReplyReceived(entryEvent);
		}
	}

}
