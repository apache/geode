package com.gemstone.gemfire.internal.tools.gfsh.app.command.task;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.ExpirationAction;
import com.gemstone.gemfire.cache.ExpirationAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryException;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.internal.ResultsBag;
import com.gemstone.gemfire.cache.query.internal.StructBag;
import com.gemstone.gemfire.cache.query.types.CollectionType;
import com.gemstone.gemfire.cache.query.types.ObjectType;
import com.gemstone.gemfire.internal.GemFireVersion;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.tools.gfsh.app.util.GfshResultsBag;
import com.gemstone.gemfire.internal.tools.gfsh.command.AbstractCommandTask;
import com.gemstone.gemfire.internal.tools.gfsh.command.CommandResults;

/**
 * QueryTask executes the specified query and returns all or a subset of the
 * result set. A subset of results is returned if the specified fetch size is
 * less than the result set size. The caller can request for the next subset of
 * the results by setting the query string to null and nextEnabled to true.
 * 
 * @author dpark
 * 
 */
public class QueryTask extends AbstractCommandTask
{
	private static final long serialVersionUID = 1L;

	static {
		staticUuid = UUID.randomUUID().toString();
	}

	private static boolean priorTo6011AndNot57 = false;

	static {
		priorTo6011AndNot57 = isPriorTo6011AndNot57();
	}

	static boolean isPriorTo6011AndNot57()
	{
		String gemfireVersion = GemFireVersion.getGemFireVersion();
		String split[] = gemfireVersion.split("\\.");
		int major = 0;
		int minor = 0;
		int update = 0;
		int update2 = 0;
		for (int i = 0; i < split.length; i++) {
			switch (i) {
			case 0:
				major = Integer.parseInt(split[i]);
				break;
			case 1:
				try {
					minor = Integer.parseInt(split[i]);
				} catch (NumberFormatException ex) {
					minor = Integer.parseInt(split[i].substring(0, 1));
				}
				break;
			case 2:
			  try {
	        update = Integer.parseInt(split[i]);
        } catch (NumberFormatException e) {
          update = 0;
        }
				break;
			case 3:				
        try {
          update2 = Integer.parseInt(split[i]);
        } catch (NumberFormatException e) {
          update2 = 0;
        }
				break;
			}
		}

		// System.out.println("GemFireVersion: " + major + "." + minor + "." +
		// update + "." + update2);

		if (major < 6) {
			return false; // 6
		} else if (minor > 0) {
			return false; // 6.1
		} else if (update < 1) {
			return true; // 6.0.0
		} else if (update > 1) {
			return false; // 6.0.2
		} else if (update2 <= 0) {
			return true; // 6.0.1.0
		} else {
			return false; // 6.0.1.1
		}

	}

	public static final byte ERROR_NONE = 0;
	public static final byte ERROR_QUERY = 1;

	private static final String REGION_NAME_RESULTS = "qr";
	private static int UUID_TIMEOUT = Integer.getInteger("QueryTask.uuidTimeout", 30); // the
																						// UUID
																						// timeout
																						// 30
																						// sec
	private static String staticUuid;

	private transient String uuid;

	private String queryString;
	private boolean nextEnabled;
	private int fetchSize = 1000;
	
	// if isPRLocalData is true then the entries from the local dataset are returned
	private boolean isPRLocalData;
	
	// if keysOnly is true then only the keys are are returned
	private boolean keysOnly;

	// Default constructor required for serialization
	public QueryTask()
	{
	}

	public QueryTask(String queryString)
	{
		this(queryString, 1000, true, false, false);
	}

	public QueryTask(String queryString, int fetchSize, boolean nextEnabled)
	{
		this(queryString, fetchSize, nextEnabled, true);
	}

	public QueryTask(String queryString, int fetchSize, boolean nextEnabled, boolean isPRLocalData)
	{
		this(queryString, fetchSize, nextEnabled, isPRLocalData, false);
	}
	
	public QueryTask(String queryString, int fetchSize, boolean nextEnabled, boolean isPRLocalData, boolean keysOnly)
	{
		this.queryString = queryString;
		this.fetchSize = fetchSize;
		this.nextEnabled = nextEnabled;
		this.isPRLocalData = isPRLocalData;
		this.keysOnly = keysOnly;
	}

	public CommandResults runTask(Object userData)
	{
		CommandResults results = execute(queryString);
		return results;
	}

	private Region getResultRegion()
	{
		Region resultRegion = super.getCommandRegion().getSubregion(REGION_NAME_RESULTS);
		if (resultRegion == null) {
			AttributesFactory factory = new AttributesFactory();
			factory.setStatisticsEnabled(true);
			factory.setScope(Scope.LOCAL);
			factory.setDataPolicy(DataPolicy.NORMAL);
			factory.setEntryIdleTimeout(new ExpirationAttributes(UUID_TIMEOUT, ExpirationAction.LOCAL_DESTROY));
			try {
				resultRegion = super.getCommandRegion().createSubregion(REGION_NAME_RESULTS, factory.create());
			} catch (Exception ex) {
				// in case another thread created it
				resultRegion = super.getCommandRegion().getSubregion(REGION_NAME_RESULTS);
			}
		}
		return resultRegion;
	}

	/**
	 * Executes the query string.
	 * 
	 * @param queryString
	 *            The query string to execute.
	 * @return The command results containing the query results.
	 */
	protected CommandResults execute(String queryString)
	{
		CommandResults results = new CommandResults();
		Cache cache = CacheFactory.getAnyInstance();

		Region resultRegion = getResultRegion();

		// Query
		try {
			Object obj = null;
			int returnedSize = 0;
			int actualSize = 0;
			boolean isPR = false;

			// next
			if (queryString == null) {
				ResultsContainer container = (ResultsContainer) resultRegion.get(uuid);
				if (container != null) {
					isPR = container.isPR;
					obj = container.getSubsetResults(getFetchSize());
					actualSize = container.getActualSize();
					returnedSize = container.getReturnedSize();
					if (container.hasNext() == false) {
						resultRegion.remove(uuid);
					}
				}

				// new query
			} else {
				if (nextEnabled) {
					resultRegion.remove(uuid);
				}
			
				String lowercase = queryString.trim().toLowerCase();
				if (lowercase.startsWith("select ")) {

					// Query
					Query query = cache.getQueryService().newQuery(queryString);
					obj = query.execute();
					if (obj instanceof SelectResults) {
						SelectResults sr = (SelectResults) obj;
						actualSize = sr.size();

						if (fetchSize != -1) {
							if (sr.size() <= fetchSize) {

								// 6.0 - 6.0.1 do not serialize ResultsBag
								// properly.
								if (isPriorTo6011AndNot57()) { // 6.0 - 6.0.1
									if (sr instanceof ResultsBag) {
										SelectResultsContainer srContainer = new SelectResultsContainer(sr);
										obj = srContainer.getSubsetResults(getFetchSize());
									} else {
										obj = sr;
									}
								} else {
									obj = sr;
								}
								returnedSize = sr.size();
							} else {
								SelectResultsContainer srContainer = new SelectResultsContainer(sr);
								obj = srContainer.getSubsetResults(getFetchSize());
								returnedSize = srContainer.returnedSize;
								if (nextEnabled) {
									resultRegion.put(uuid, srContainer);
								}
							}
						} else {
							returnedSize = sr.size();
						}
					}

				} else {

					// Region

					String regionPath = queryString;
					Region region = cache.getRegion(regionPath);
					if (region == null) {
						results.setCode(ERROR_QUERY);
						results.setCodeMessage("Invalid region path. Unable to query data.");
					} else {
						// Return region keys or entries
						isPR = region instanceof PartitionedRegion;
						Region r;
						Set resultSet = null;
						if (isPRLocalData && isPR) {
							PartitionedRegion pr = (PartitionedRegion) region;
//							r = new LocalDataSet(pr, pr.getDataStore().getAllLocalPrimaryBucketIds());
							if (pr.getDataStore() == null) {
								// PROXY - no data store
								results.setCodeMessage("No data store");
								return results;
							}
							List<Integer> bucketIdList = pr.getDataStore().getLocalPrimaryBucketsListTestOnly();
							resultSet = new HashSet();
							for (Integer bucketId : bucketIdList) {
								BucketRegion bucketRegion;
								try {
									bucketRegion = pr.getDataStore().getInitializedBucketForId(null, bucketId);
									Set set;
									if (keysOnly) {
										set = bucketRegion.keySet();
									} else {
										set = bucketRegion.entrySet();
									}
									for (Object object : set) {
										resultSet.add(object);
									}
								} catch (ForceReattemptException e) {
									// ignore
								}
							}
						} else {
							r = region;
							if (keysOnly) {
								resultSet = r.keySet();
							} else {
								resultSet = r.entrySet();
							}
						}
						actualSize = resultSet.size();
						RegionContainer regionContainer = new RegionContainer(resultSet, keysOnly, isPR);
						obj = regionContainer.getSubsetResults(getFetchSize());
						returnedSize = regionContainer.getReturnedSize();
						if (nextEnabled && regionContainer.hasNext()) {
							resultRegion.put(uuid, regionContainer);
						}
					}
				}
			}

			results.setDataObject(new QueryResults(obj, actualSize, fetchSize, returnedSize, isPR));

		} catch (QueryException e) {
			cache.getLogger().warning(e);
			results.setCode(ERROR_QUERY);
			results.setCodeMessage("Unable to execute command task. Invalid query.");
			results.setException(e);
		}
		return results;
	}

	public String getQuery()
	{
		return queryString;
	}

	public void setQuery(String queryString)
	{
		this.queryString = queryString;
	}

	/**
	 * Returns the fetch size. The default is 1000. If -1, fetches all.
	 * 
	 * @return fetch size
	 */
	public int getFetchSize()
	{
		return fetchSize;
	}

	/**
	 * Sets the fetch size. The default is 1000.
	 * 
	 * @param fetchSize
	 *            The fetch size. If -1, fetches all.
	 */
	public void setFetchSize(int fetchSize)
	{
		this.fetchSize = fetchSize;
	}

	public void fromData(DataInput input) throws IOException, ClassNotFoundException
	{
		super.fromData(input);
		uuid = DataSerializer.readString(input);
		queryString = DataSerializer.readString(input);
		nextEnabled = input.readBoolean();
		isPRLocalData = input.readBoolean();
		fetchSize = input.readInt();
		keysOnly = input.readBoolean();
	}

	public void toData(DataOutput output) throws IOException
	{
		super.toData(output);
		DataSerializer.writeString(staticUuid, output);
		DataSerializer.writeString(queryString, output);
		output.writeBoolean(nextEnabled);
		output.writeBoolean(isPRLocalData);
		output.writeInt(fetchSize);
		output.writeBoolean(keysOnly);
	}

	abstract class ResultsContainer
	{
		boolean isPR = false;
		
		protected abstract Object getSubsetResults(int fetchSize);

		protected abstract boolean hasNext();

		protected abstract int getActualSize();

		protected abstract int getReturnedSize();

	}

	class SelectResultsContainer extends ResultsContainer
	{
		SelectResults sr;
		Iterator iterator;
		int returnedSize = 0;;

		SelectResultsContainer(SelectResults sr)
		{
			this.sr = sr;
			iterator = sr.iterator();
		}

		protected Object getSubsetResults(int fetchSize)
		{
			SelectResults sr2;
			CollectionType type = sr.getCollectionType();
			ObjectType elementType = type.getElementType();
			if (elementType.isStructType()) {
				sr2 = new StructBag();
			} else {
				if (isPriorTo6011AndNot57()) { // 6.0 - 6.0.1
					sr2 = new GfshResultsBag();
				} else {
					sr2 = new ResultsBag();
				}
			}
			sr2.setElementType(elementType);

			int count = 0;
			while (count < fetchSize && iterator.hasNext()) {
				Object object = (Object) iterator.next();
				sr2.add(object);
				count++;
			}
			returnedSize += count;
			return sr2;
		}

		protected boolean hasNext()
		{
			return iterator.hasNext();
		}

		protected int getActualSize()
		{
			return sr.size();
		}

		protected int getReturnedSize()
		{
			return returnedSize;
		}
	}

	class RegionContainer extends ResultsContainer
	{
		Set resultSet;
		Iterator iterator;
		int returnedSize = 0;
		boolean keysOnly;

		RegionContainer(Set resultSet, boolean keysOnly, boolean isPR)
		{
			this.resultSet = resultSet;
			this.keysOnly = keysOnly;
			super.isPR = isPR;
			iterator = resultSet.iterator();
		}

		protected Object getSubsetResults(int fetchSize)
		{
			int count = 0;
			Object retval;
			
			if (keysOnly) {
				ArrayList list = new ArrayList();
				while (count < fetchSize && iterator.hasNext()) {
					Object key = iterator.next();
					list.add(key);
					count++;
				}
				retval = list;
			} else {
				HashMap map = new HashMap();
				while (count < fetchSize && iterator.hasNext()) {
					Region.Entry entry = (Region.Entry)iterator.next();
					map.put(entry.getKey(), entry.getValue());
					count++;
				}
				retval = map;
			}
			returnedSize += count;
			
			return retval;
		}

		protected boolean hasNext()
		{
			return iterator.hasNext();
		}

		protected int getActualSize()
		{
			return resultSet.size();
		}

		protected int getReturnedSize()
		{
			return returnedSize;
		}
	}
}
