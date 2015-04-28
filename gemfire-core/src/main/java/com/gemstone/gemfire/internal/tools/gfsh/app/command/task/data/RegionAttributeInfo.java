package com.gemstone.gemfire.internal.tools.gfsh.app.command.task.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.ExpirationAction;
import com.gemstone.gemfire.cache.ExpirationAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;

/**
 * A data class that contains region attribute information.
 * @author dpark
 *
 */
public class RegionAttributeInfo implements DataSerializable
{
	private static final long serialVersionUID = 1L;
	
	// Region attributes
	public static final String CONCURRENCY_LEVEL = "concurrency-level";
	public static final String DATA_POLICY = "data-policy";
	public static final String EARLY_ACK = "early-ack";
	public static final String ENABLE_ASYNC_CONFLATION = "enable-async-conflation";
	public static final String ENABLE_GATEWAY = "enable-gateway";
	public static final String ENABLE_SUBSCRIPTION_CONFLATION = "enable-subscription-conflation";
	public static final String HUB_ID = "hub-id";
	public static final String IGNORE_JTA = "ignore-jta";
	public static final String INDEX_UPDATE_TYPE = "index-update-type";
	public static final String INITIAL_CAPACITY = "initial-capacity";
	public static final String IS_LOCK_GRANTOR = "is-lock-grantor";
	public static final String LOAD_FACTOR = "load-factor";
	public static final String MULTICAST_ENABLED = "multicast-enabled";
	public static final String PUBLISHER = "publisher";
	public static final String SCOPE = "scope";
	public static final String STATISTICS_ENABLED = "statistics-enabled";
	
	// Region element attributes
	// region-time-to-live
	public static final String REGION_TIME_TO_LIVE_TIMEOUT = "region-time-to-live.timeout";
	public static final String REGION_TIME_TO_LIVE_ACTION = "region-time-to-live.action";
	
	// region-idle-time
	public static final String REGION_IDLE_TIME_TIMEOUT = "region-idle-time.timeout";
	public static final String REGION_IDLE_TIME_ACTION = "region-idle-time.action";
	
	// entry-time-to-live
	public static final String ENTRY_TIME_TO_LIVE_TIMEOUT = "entry-time-to-live.timeout";
	public static final String ENTRY_TIME_TO_LIVE_ACTION = "entry-time-to-live.action";
	
	// entry-idle-time
	public static final String ENTRY_IDLE_TIME_TIMEOUT = "entry-idle-time.timeout";
	public static final String ENTRY_IDLE_TIME_ACTION = "entry-idle-time.action";
	
	// disk-dirs
	public static final String DISK_DIRS_DISK_DIR = "disk-dirs.disk-dir"; // 1, many
	
	// disk-write-attributes
	public static final String DISK_WRITE_ATTRIBUTES_MAX_OPLOG_SIZE = "disk-write-attributes.max-oplog-size";
	public static final String DISK_WRITE_ATTRIBUTES_ROLL_OPLOGS = "disk-write-attributes.roll-oplogs";
	public static final String DISK_WRITE_ATTRIBUTES_ASYNCHRONOUS_WRITES_TIME_INTERVAL = "disk-write-attributes.asynchronous-writes.time-interval";
	public static final String DISK_WRITE_ATTRIBUTES_ASYNCHRONOUS_WRITES_BYTES_THRESHOLD = "disk-write-attributes.asynchronous-writes.bytes-threshold";
	public static final String DISK_WRITE_ATTRIBUTES_SYNCHRONOUS_WRITES = "disk-write-attributes.synchronous-writes"; // true|false
	
	// membership-attributes
	public static final String MEMBERSHIP_ATTRIBUTES_LOSS_ACTION = "membership-attributes.loss-action"; // 0,1
	public static final String MEMBERSHIP_ATTRIBUTES_RESUMPTION_ACTION = "membership-attributes.resumption-action"; // 0,1
	public static final String MEMBERSHIP_ATTRIBUTES_REQUIRED_ROLE = "membership-attributes.resumption-action"; // 1, many
	
	// subscription-attributes
	public static final String SUBSCRIPTION_ATTRIBUTES_INTEREST_POLICY = "subscription-attributes.interest-policy"; // 0,1
	
	// eviction-attributes.lru-entry-count
	public static final String EVICTION_ATTRIBUTES_LRU_ENTRY_COUNT_ACTION = "eviction-attributes.lru-entry-count.action";
	public static final String EVICTION_ATTRIBUTES_LRU_ENTRY_COUNT_MAXIMUM = "eviction-attributes.lru-entry-count.maximum";
	
	// eviction-attributes.lru-memory-size 
	public static final String EVICTION_ATTRIBUTES_LRU_MEMORY_SIZE_ACTION = "eviction-attributes.lru-memory-size.action";
	public static final String EVICTION_ATTRIBUTES_LRU_MEMORY_SIZE_MAXIMUM = "eviction-attributes.lru-memory-size.maximum";
	public static final String EVICTION_ATTRIBUTES_LRU_MEMORY_SIZE_CLASS_NAME = "eviction-attributes.lru-memory-size.class-name";
	public static final String EVICTION_ATTRIBUTES_LRU_MEMORY_SIZE_PARAMETER = "eviction-attributes.lru-memory-size.parameter";
	public static final String EVICTION_ATTRIBUTES_LRU_MEMORY_SIZE_PARAMETER_STRING = "eviction-attributes.lru-memory-size.parameter.string";
	public static final String EVICTION_ATTRIBUTES_LRU_MEMORY_SIZE_PARAMETER_DECLARABLE = "eviction-attributes.lru-memory-size.parameter.declarable";
	
	// eviction-attributes.lru-heap-percentage
	public static final String EVICTION_ATTRIBUTES_LRU_HEAP_PERCENTAGE_ACTION = "eviction-attributes.lru-heap-percentage.action";
	public static final String EVICTION_ATTRIBUTES_LRU_HEAP_PERCENTAGE_CLASS_NAME = "eviction-attributes.lru-heap-percentage.class-name";
	public static final String EVICTION_ATTRIBUTES_LRU_HEAP_PERCENTAGE_PARAMETER = "eviction-attributes.lru-heap-percentage.parameter";
	public static final String EVICTION_ATTRIBUTES_LRU_HEAP_PERCENTAGE_PARAMETER_STRING = "eviction-attributes.lru-heap-percentage.parameter.string";
	public static final String EVICTION_ATTRIBUTES_LRU_HEAP_PERCENTAGE_PARAMETER_DECLARABLE = "eviction-attributes.lru-heap-percentage.parameter.declarable";
	
	// key-constraint
	public static final String KEY_CONTRATINT = "key-constraint";
	
	// value-constraint
	public static final String VALUE_CONTRATINT = "value-constraint";
	
	// cache-listener
	public static final String CACHE_LISTENER_CLASS_NAME = "cache-listener.class-name";
	public static final String CACHE_LISTENER_PARAMETER_NAME = "cache-listener.parameter.name";
	public static final String CACHE_LISTENER_PARAMETER_STRING = "cache-listener.parameter.string";
	public static final String CACHE_LISTENER_PARAMETER_DECLARABLE_CLASS_NAME = "cache-listener.parameter.declarable.class-name";
	public static final String CACHE_LISTENER_PARAMETER_DECLARABLE_PARAMETER_NAME = "cache-listener.parameter.declarable.parameter.name";
	public static final String CACHE_LISTENER_PARAMETER_DECLARABLE_PARAMETER_STRING = "cache-listener.parameter.declarable.parameter.string";
	
	// Partition attributes
	public static final String LOCAL_MAX_MEMORY = "local-max-memory";
	public static final String REDUNDANT_COPIES = "redundant-copies";
	public static final String TOTAL_MAX_MEMORY = "total-max-memory";
	public static final String TOTAL_NUM_BUCKETS = "total-num-buckets";
	
	private int versionId = 1;
	
	private HashMap attrProperties = new HashMap();
	
	public RegionAttributeInfo() {}
	
	public RegionAttributeInfo(Properties attrProperties)
	{
		this.attrProperties.putAll(attrProperties);
	}
	
	public void setAttribute(String attributeName, String value)
	{
		attrProperties.put(attributeName, value);
	}
	
	public String getAttribute(String attributeName)
	{
		return (String)attrProperties.get(attributeName);
	}
	
	public RegionAttributes createRegionAttributes() 
	{
		AttributesFactory factory = new AttributesFactory();
		PartitionAttributesFactory partitionAttrFactory = null;
		
		Set entrySet = attrProperties.entrySet();
		for (Iterator<Entry<String, String>> iterator = entrySet.iterator(); iterator.hasNext();) {
			Entry<String, String> entry = iterator.next();
			String attr = (String)entry.getKey();
			String value = (String)entry.getValue();
			value = value.replace('-', '_');
			if (attr.equals(CONCURRENCY_LEVEL)) {
				factory.setConcurrencyLevel(Integer.parseInt(value));
			} else if (attr.equals(DATA_POLICY)) {
				if (value.equalsIgnoreCase(DataPolicy.EMPTY.toString())) {
					factory.setDataPolicy(DataPolicy.EMPTY);
				} else if (value.equalsIgnoreCase(DataPolicy.NORMAL.toString())) {
					factory.setDataPolicy(DataPolicy.NORMAL);
				} else if (value.equalsIgnoreCase(DataPolicy.PARTITION.toString())) {
					factory.setDataPolicy(DataPolicy.PARTITION);
				} else if (value.equalsIgnoreCase(DataPolicy.PERSISTENT_REPLICATE.toString())) {
					factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
				} else if (value.equalsIgnoreCase(DataPolicy.PRELOADED.toString())) {
					factory.setDataPolicy(DataPolicy.PRELOADED);
				} else if (value.equalsIgnoreCase(DataPolicy.REPLICATE.toString())) {
					factory.setDataPolicy(DataPolicy.REPLICATE);
				}
			} else if (attr.equals(EARLY_ACK)) {
				factory.setEarlyAck(Boolean.parseBoolean(value));
			} else if (attr.equals(ENABLE_ASYNC_CONFLATION)) {
				factory.setEnableAsyncConflation(Boolean.parseBoolean(value));
			} else if (attr.equals(ENABLE_SUBSCRIPTION_CONFLATION)) {
				factory.setEnableSubscriptionConflation(Boolean.parseBoolean(value));
			} else if (attr.equals(IGNORE_JTA)) {
				factory.setIgnoreJTA(Boolean.parseBoolean(value));
			} else if (attr.equals(INDEX_UPDATE_TYPE)) {
				factory.setIndexMaintenanceSynchronous(value.equals("asynchronous"));
			} else if (attr.equals(INITIAL_CAPACITY)) {
				factory.setInitialCapacity(Integer.parseInt(value));
			} else if (attr.equals(IS_LOCK_GRANTOR)) {
				factory.setLockGrantor(Boolean.parseBoolean(value));
			} else if (attr.equals(LOAD_FACTOR)) {
				factory.setLoadFactor(Float.parseFloat(value));
			} else if (attr.equals(MULTICAST_ENABLED)) {
				factory.setMulticastEnabled(Boolean.parseBoolean(value));	
			} else if (attr.equals(PUBLISHER)) {
				factory.setPublisher(Boolean.parseBoolean(value));	
			} else if (attr.equals(SCOPE)) {
				if (value.equalsIgnoreCase(Scope.DISTRIBUTED_ACK.toString())) {
					factory.setScope(Scope.DISTRIBUTED_ACK);
				} else if (value.equalsIgnoreCase(Scope.DISTRIBUTED_NO_ACK.toString())) {
					factory.setScope(Scope.DISTRIBUTED_NO_ACK);
				} else if (value.equalsIgnoreCase(Scope.GLOBAL.toString())) {
					factory.setScope(Scope.GLOBAL);
				} else if (value.equalsIgnoreCase(Scope.LOCAL.toString())) {
					factory.setScope(Scope.LOCAL);
				}
			} else if (attr.equals(STATISTICS_ENABLED)) {
				factory.setStatisticsEnabled(Boolean.parseBoolean(value));	
				
			// Partition attributes
			} else if (attr.equals(LOCAL_MAX_MEMORY)) {
				if (partitionAttrFactory == null) {
					partitionAttrFactory = new PartitionAttributesFactory();
				}
				partitionAttrFactory.setLocalMaxMemory(Integer.parseInt(value));
			} else if (attr.equals(REDUNDANT_COPIES)) {
				if (partitionAttrFactory == null) {
					partitionAttrFactory = new PartitionAttributesFactory();
				}
				partitionAttrFactory.setRedundantCopies(Integer.parseInt(value));
			} else if (attr.equals(TOTAL_MAX_MEMORY)) {
				if (partitionAttrFactory == null) {
					partitionAttrFactory = new PartitionAttributesFactory();
				}
				partitionAttrFactory.setTotalMaxMemory(Integer.parseInt(value));
			} else if (attr.equals(TOTAL_NUM_BUCKETS)) {
				if (partitionAttrFactory == null) {
					partitionAttrFactory = new PartitionAttributesFactory();
				}
				partitionAttrFactory.setTotalNumBuckets(Integer.parseInt(value));
			
			
			// region attributes elements - additions (9/19/09)
			} else if (attr.equals(ENTRY_IDLE_TIME_TIMEOUT)) {
				int timeout = Integer.parseInt(value);
				String action = (String)attrProperties.get(ENTRY_IDLE_TIME_ACTION);
				factory.setEntryIdleTimeout(new ExpirationAttributes(timeout, getExpirationAction(action)));
			} else if (attr.equals(ENTRY_TIME_TO_LIVE_TIMEOUT)) {
				int timeout = Integer.parseInt(value);
				String action = (String)attrProperties.get(ENTRY_TIME_TO_LIVE_ACTION);
				factory.setEntryTimeToLive(new ExpirationAttributes(timeout, getExpirationAction(action)));
			} else if (attr.equals(REGION_IDLE_TIME_TIMEOUT)) {
				int timeout = Integer.parseInt(value);
				String action = (String)attrProperties.get(REGION_IDLE_TIME_ACTION);
				factory.setRegionIdleTimeout(new ExpirationAttributes(timeout, getExpirationAction(action)));
			} else if (attr.equals(REGION_TIME_TO_LIVE_TIMEOUT)) {
				int timeout = Integer.parseInt(value);
				String action = (String)attrProperties.get(REGION_TIME_TO_LIVE_ACTION);
				factory.setRegionTimeToLive(new ExpirationAttributes(timeout, getExpirationAction(action)));
			}
			
		}
		
		if (partitionAttrFactory != null) {
			factory.setPartitionAttributes(partitionAttrFactory.create());
		}
		
		return factory.create();
	}
	
	private ExpirationAction getExpirationAction(String action)
	{
		if (action == null) {
			return ExpirationAttributes.DEFAULT.getAction();
		}
		action = action.replace('-', '_');
		if (action.equalsIgnoreCase(ExpirationAction.DESTROY.toString())) {
			return ExpirationAction.DESTROY;
		} else if (action.equalsIgnoreCase(ExpirationAction.INVALIDATE.toString())) {
				return ExpirationAction.INVALIDATE;
		} else if (action.equalsIgnoreCase(ExpirationAction.LOCAL_DESTROY.toString())) {
			return ExpirationAction.LOCAL_DESTROY;
		} else if (action.equalsIgnoreCase(ExpirationAction.LOCAL_INVALIDATE.toString())) {
			return ExpirationAction.LOCAL_INVALIDATE;
		} else {
			return ExpirationAttributes.DEFAULT.getAction();
		}
	}

	public void fromData(DataInput in) throws IOException, ClassNotFoundException
	{
		versionId = in.readInt();
		attrProperties = (HashMap)DataSerializer.readObject(in);
	}

	public void toData(DataOutput out) throws IOException
	{
		out.writeInt(versionId);
		DataSerializer.writeObject(attrProperties, out);
	}

}
