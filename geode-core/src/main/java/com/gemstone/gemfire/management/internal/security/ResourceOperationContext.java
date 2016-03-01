/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.management.internal.security;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.cache.operations.OperationContext;

/**
 * This is base class for OperationContext for resource (JMX and CLI) operations
 *
 */
public abstract class ResourceOperationContext extends OperationContext {
	
  private boolean isPostOperation=false;
  private Object opResult = null;

	 public static class ResourceOperationCode {
		 
    private static final int OP_ALTER_REGION = 1;
    private static final int OP_ALTER_RUNTIME = 2;
    private static final int OP_BACKUP_DISKSTORE = 3;
    private static final int OP_CHANGE_ALERT_LEVEL = 4;
    private static final int OP_CLOSE_DURABLE_CLIENT = 5;
    private static final int OP_CLOSE_DURABLE_CQ = 6;
    private static final int OP_COMPACT_DISKSTORE = 7;
    private static final int OP_CONFIGURE_PDX = 8;
    private static final int OP_CREATE_AEQ = 9;
    private static final int OP_CREATE_DISKSTORE = 10;
    private static final int OP_CREATE_GW_RECEIVER = 11;
    private static final int OP_CREATE_GW_SENDER = 12;
    private static final int OP_CREATE_INDEX = 13;
    private static final int OP_CREATE_REGION = 14;
    private static final int OP_DEPLOY = 15;
    private static final int OP_DESTROY_DISKSTORE = 16;
    private static final int OP_DESTROY_FUNCTION = 17;
    private static final int OP_DESTROY_INDEX = 18;
    private static final int OP_DESTROY_REGION = 19;
    private static final int OP_EXECUTE_FUNCTION = 20;
    private static final int OP_EXPORT_CONFIG = 21;
    private static final int OP_EXPORT_DATA = 22;
    private static final int OP_EXPORT_LOGS = 23;
    private static final int OP_EXPORT_OFFLINE_DISKSTORE = 24;
    private static final int OP_EXPORT_STACKTRACE = 25;
    private static final int OP_GC = 26;
    private static final int OP_GET = 27;
    private static final int OP_IMPORT_CONFIG = 28;
    private static final int OP_IMPORT_DATA = 29;
    private static final int OP_LIST_DS = 30;
    private static final int OP_LOAD_BALANCE_GW_SENDER = 31;
    private static final int OP_LOCATE_ENTRY = 32;
    private static final int OP_NETSTAT = 33;
    private static final int OP_PAUSE_GW_SENDER = 34;
    private static final int OP_PUT = 35;
    private static final int OP_QUERY = 36;
    private static final int OP_REBALANCE = 37;
    private static final int OP_REMOVE = 38;
    private static final int OP_RENAME_PDX = 39;
    private static final int OP_RESUME_GW_SENDER = 40;
    private static final int OP_REVOKE_MISSING_DISKSTORE = 41;
    private static final int OP_SHOW_DEADLOCKS = 42;
    private static final int OP_SHOW_LOG = 43;
    private static final int OP_SHOW_METRICS = 44;
    private static final int OP_SHOW_MISSING_DISKSTORES = 45;
    private static final int OP_SHOW_SUBSCRIPTION_QUEUE_SIZE = 46;
    private static final int OP_SHUTDOWN = 47;
    private static final int OP_STOP_GW_RECEIVER = 48;
    private static final int OP_STOP_GW_SENDER = 49;
    private static final int OP_UNDEPLOY = 50;
    private static final int OP_BACKUP_MEMBERS = 51;
    private static final int OP_ROLL_DISKSTORE = 52;
    private static final int OP_FORCE_COMPACTION = 53;
    private static final int OP_FORCE_ROLL = 54;
    private static final int OP_FLUSH_DISKSTORE = 55;
    private static final int OP_START_GW_RECEIVER = 56;
    private static final int OP_START_GW_SENDER = 57;
    private static final int OP_BECOME_LOCK_GRANTOR = 58;
    private static final int OP_START_MANAGER = 59;
    private static final int OP_STOP_MANAGER = 60;
    private static final int OP_CREATE_MANAGER = 61;
    private static final int OP_STOP_CONTINUOUS_QUERY = 62;
    private static final int OP_SET_DISK_USAGE = 63;
    private static final int OP_CREATE_HDFS_STORE = 64;
    private static final int OP_ALTER_HDFS_STORE = 65;
    private static final int OP_DESTROY_HDFS_STORE = 66;
	    
    private static final int OP_PULSE_DASHBOARD = 92;
    private static final int OP_PULSE_DATABROWSER = 93;
    private static final int OP_PULSE_WEBGFSH = 94;
    private static final int OP_PULSE_ADMIN_V1 = 95;
	    
    private static final int OP_DATA_READ = 96;
    private static final int OP_DATA_WRITE = 97;
    private static final int OP_MONITOR = 98;
    private static final int OP_ADMIN = 99;
	    
    private static final ResourceOperationCode[] VALUES = new ResourceOperationCode[100];
    private static final Map<String, ResourceOperationCode> OperationNameMap = new HashMap<String, ResourceOperationCode>();

	    
    public static final ResourceOperationCode ALTER_REGION  = new ResourceOperationCode(ResourceConstants.ALTER_REGION, OP_ALTER_REGION);
    public static final ResourceOperationCode ALTER_RUNTIME = new ResourceOperationCode(ResourceConstants.ALTER_RUNTIME, OP_ALTER_RUNTIME);
    public static final ResourceOperationCode BACKUP_DISKSTORE = new ResourceOperationCode(ResourceConstants.BACKUP_DISKSTORE, OP_BACKUP_DISKSTORE);
    public static final ResourceOperationCode CHANGE_ALERT_LEVEL = new ResourceOperationCode(ResourceConstants.CHANGE_ALERT_LEVEL, OP_CHANGE_ALERT_LEVEL);
    public static final ResourceOperationCode CLOSE_DURABLE_CLIENT = new ResourceOperationCode(ResourceConstants.CLOSE_DURABLE_CLIENT, OP_CLOSE_DURABLE_CLIENT);
    public static final ResourceOperationCode CLOSE_DURABLE_CQ = new ResourceOperationCode(ResourceConstants.CLOSE_DURABLE_CQ, OP_CLOSE_DURABLE_CQ);
    public static final ResourceOperationCode COMPACT_DISKSTORE = new ResourceOperationCode(ResourceConstants.COMPACT_DISKSTORE, OP_COMPACT_DISKSTORE);
    public static final ResourceOperationCode CONFIGURE_PDX = new ResourceOperationCode(ResourceConstants.CONFIGURE_PDX, OP_CONFIGURE_PDX);
    public static final ResourceOperationCode CREATE_AEQ = new ResourceOperationCode(ResourceConstants.CREATE_AEQ, OP_CREATE_AEQ);
    public static final ResourceOperationCode CREATE_DISKSTORE = new ResourceOperationCode(ResourceConstants.CREATE_DISKSTORE, OP_CREATE_DISKSTORE);
    public static final ResourceOperationCode CREATE_GW_RECEIVER = new ResourceOperationCode(ResourceConstants.CREATE_GW_RECEIVER, OP_CREATE_GW_RECEIVER);
    public static final ResourceOperationCode CREATE_GW_SENDER = new ResourceOperationCode(ResourceConstants.CREATE_GW_SENDER, OP_CREATE_GW_SENDER);
    public static final ResourceOperationCode CREATE_INDEX = new ResourceOperationCode(ResourceConstants.CREATE_INDEX, OP_CREATE_INDEX);
    public static final ResourceOperationCode CREATE_REGION = new ResourceOperationCode(ResourceConstants.CREATE_REGION, OP_CREATE_REGION);
    public static final ResourceOperationCode DEPLOY = new ResourceOperationCode(ResourceConstants.DEPLOY, OP_DEPLOY);
    public static final ResourceOperationCode DESTROY_DISKSTORE = new ResourceOperationCode(ResourceConstants.DESTROY_DISKSTORE, OP_DESTROY_DISKSTORE);
    public static final ResourceOperationCode DESTROY_FUNCTION = new ResourceOperationCode(ResourceConstants.DESTROY_FUNCTION, OP_DESTROY_FUNCTION);
    public static final ResourceOperationCode DESTROY_INDEX = new ResourceOperationCode(ResourceConstants.DESTROY_INDEX, OP_DESTROY_INDEX);
    public static final ResourceOperationCode DESTROY_REGION = new ResourceOperationCode(ResourceConstants.DESTROY_REGION, OP_DESTROY_REGION);
    public static final ResourceOperationCode EXECUTE_FUNCTION = new ResourceOperationCode(ResourceConstants.EXECUTE_FUNCTION, OP_EXECUTE_FUNCTION);
    public static final ResourceOperationCode EXPORT_CONFIG = new ResourceOperationCode(ResourceConstants.EXPORT_CONFIG, OP_EXPORT_CONFIG);
    public static final ResourceOperationCode EXPORT_DATA = new ResourceOperationCode(ResourceConstants.EXPORT_DATA, OP_EXPORT_DATA);
    public static final ResourceOperationCode EXPORT_LOGS = new ResourceOperationCode(ResourceConstants.EXPORT_LOGS, OP_EXPORT_LOGS);
    public static final ResourceOperationCode EXPORT_OFFLINE_DISKSTORE = new ResourceOperationCode(ResourceConstants.EXPORT_OFFLINE_DISKSTORE, OP_EXPORT_OFFLINE_DISKSTORE);
    public static final ResourceOperationCode EXPORT_STACKTRACE = new ResourceOperationCode(ResourceConstants.EXPORT_STACKTRACE, OP_EXPORT_STACKTRACE);
    public static final ResourceOperationCode GC = new ResourceOperationCode(ResourceConstants.GC, OP_GC);
    public static final ResourceOperationCode GET = new ResourceOperationCode(ResourceConstants.GET, OP_GET);
    public static final ResourceOperationCode IMPORT_CONFIG = new ResourceOperationCode(ResourceConstants.IMPORT_CONFIG, OP_IMPORT_CONFIG);
    public static final ResourceOperationCode IMPORT_DATA = new ResourceOperationCode(ResourceConstants.IMPORT_DATA, OP_IMPORT_DATA);
	    public static final ResourceOperationCode LIST_DS = new ResourceOperationCode(ResourceConstants.LIST_DS, OP_LIST_DS);
    public static final ResourceOperationCode LOAD_BALANCE_GW_SENDER = new ResourceOperationCode(ResourceConstants.LOAD_BALANCE_GW_SENDER, OP_LOAD_BALANCE_GW_SENDER);
    public static final ResourceOperationCode LOCATE_ENTRY = new ResourceOperationCode(ResourceConstants.LOCATE_ENTRY, OP_LOCATE_ENTRY);
    public static final ResourceOperationCode NETSTAT = new ResourceOperationCode(ResourceConstants.NETSTAT, OP_NETSTAT);
    public static final ResourceOperationCode PAUSE_GW_SENDER = new ResourceOperationCode(ResourceConstants.PAUSE_GW_SENDER, OP_PAUSE_GW_SENDER);
    public static final ResourceOperationCode PUT = new ResourceOperationCode(ResourceConstants.PUT, OP_PUT);
    public static final ResourceOperationCode QUERY = new ResourceOperationCode(ResourceConstants.QUERY, OP_QUERY);
    public static final ResourceOperationCode REBALANCE = new ResourceOperationCode(ResourceConstants.REBALANCE, OP_REBALANCE);
    public static final ResourceOperationCode REMOVE = new ResourceOperationCode(ResourceConstants.REMOVE, OP_REMOVE);
    public static final ResourceOperationCode RENAME_PDX = new ResourceOperationCode(ResourceConstants.RENAME_PDX, OP_RENAME_PDX);
    public static final ResourceOperationCode RESUME_GW_SENDER = new ResourceOperationCode(ResourceConstants.RESUME_GW_SENDER, OP_RESUME_GW_SENDER);
    public static final ResourceOperationCode REVOKE_MISSING_DISKSTORE = new ResourceOperationCode(ResourceConstants.REVOKE_MISSING_DISKSTORE, OP_REVOKE_MISSING_DISKSTORE);
    public static final ResourceOperationCode SHOW_DEADLOCKS = new ResourceOperationCode(ResourceConstants.SHOW_DEADLOCKS, OP_SHOW_DEADLOCKS);
    public static final ResourceOperationCode SHOW_LOG = new ResourceOperationCode(ResourceConstants.SHOW_LOG, OP_SHOW_LOG);
    public static final ResourceOperationCode SHOW_METRICS = new ResourceOperationCode(ResourceConstants.SHOW_METRICS, OP_SHOW_METRICS);
    public static final ResourceOperationCode SHOW_MISSING_DISKSTORES = new ResourceOperationCode(ResourceConstants.SHOW_MISSING_DISKSTORES, OP_SHOW_MISSING_DISKSTORES);
    public static final ResourceOperationCode SHOW_SUBSCRIPTION_QUEUE_SIZE = new ResourceOperationCode(ResourceConstants.SHOW_SUBSCRIPTION_QUEUE_SIZE, OP_SHOW_SUBSCRIPTION_QUEUE_SIZE);
    public static final ResourceOperationCode SHUTDOWN = new ResourceOperationCode(ResourceConstants.SHUTDOWN, OP_SHUTDOWN);
    public static final ResourceOperationCode STOP_GW_RECEIVER = new ResourceOperationCode(ResourceConstants.STOP_GW_RECEIVER, OP_STOP_GW_RECEIVER);
    public static final ResourceOperationCode STOP_GW_SENDER = new ResourceOperationCode(ResourceConstants.STOP_GW_SENDER, OP_STOP_GW_SENDER);
    public static final ResourceOperationCode UNDEPLOY = new ResourceOperationCode(ResourceConstants.UNDEPLOY, OP_UNDEPLOY);
    public static final ResourceOperationCode BACKUP_MEMBERS = new ResourceOperationCode(ResourceConstants.BACKUP_MEMBERS, OP_BACKUP_MEMBERS);
    public static final ResourceOperationCode ROLL_DISKSTORE = new ResourceOperationCode(ResourceConstants.ROLL_DISKSTORE, OP_ROLL_DISKSTORE);
    public static final ResourceOperationCode FORCE_COMPACTION = new ResourceOperationCode(ResourceConstants.FORCE_COMPACTION, OP_FORCE_COMPACTION);
    public static final ResourceOperationCode FORCE_ROLL = new ResourceOperationCode(ResourceConstants.FORCE_ROLL, OP_FORCE_ROLL);
    public static final ResourceOperationCode FLUSH_DISKSTORE = new ResourceOperationCode(ResourceConstants.FLUSH_DISKSTORE, OP_FLUSH_DISKSTORE);
    public static final ResourceOperationCode START_GW_RECEIVER = new ResourceOperationCode(ResourceConstants.START_GW_RECEIVER, OP_START_GW_RECEIVER);
    public static final ResourceOperationCode START_GW_SENDER = new ResourceOperationCode(ResourceConstants.START_GW_SENDER, OP_START_GW_SENDER);
    public static final ResourceOperationCode BECOME_LOCK_GRANTOR = new ResourceOperationCode(ResourceConstants.BECOME_LOCK_GRANTOR, OP_BECOME_LOCK_GRANTOR);
    public static final ResourceOperationCode START_MANAGER = new ResourceOperationCode(ResourceConstants.START_MANAGER, OP_START_MANAGER);
    public static final ResourceOperationCode STOP_MANAGER = new ResourceOperationCode(ResourceConstants.STOP_MANAGER, OP_STOP_MANAGER);
    public static final ResourceOperationCode CREATE_MANAGER = new ResourceOperationCode(ResourceConstants.CREATE_MANAGER, OP_CREATE_MANAGER);
    public static final ResourceOperationCode STOP_CONTINUOUS_QUERY = new ResourceOperationCode(ResourceConstants.STOP_CONTINUOUS_QUERY, OP_STOP_CONTINUOUS_QUERY);
    public static final ResourceOperationCode SET_DISK_USAGE = new ResourceOperationCode(ResourceConstants.SET_DISK_USAGE, OP_SET_DISK_USAGE);
    public static final ResourceOperationCode CREATE_HDFS_STORE = new ResourceOperationCode(ResourceConstants.CREATE_HDFS_STORE, OP_CREATE_HDFS_STORE);
    public static final ResourceOperationCode ALTER_HDFS_STORE = new ResourceOperationCode(ResourceConstants.ALTER_HDFS_STORE, OP_ALTER_HDFS_STORE);
    public static final ResourceOperationCode DESTROY_HDFS_STORE = new ResourceOperationCode(ResourceConstants.DESTROY_HDFS_STORE, OP_DESTROY_HDFS_STORE);

	    
    public static final ResourceOperationCode PULSE_DASHBOARD = new ResourceOperationCode(
        ResourceConstants.PULSE_DASHBOARD, OP_PULSE_DASHBOARD);
    public static final ResourceOperationCode PULSE_DATABROWSER = new ResourceOperationCode(
        ResourceConstants.PULSE_DATABROWSER, OP_PULSE_DATABROWSER);
    public static final ResourceOperationCode PULSE_WEBGFSH = new ResourceOperationCode(
        ResourceConstants.PULSE_WEBGFSH, OP_PULSE_WEBGFSH);
    public static final ResourceOperationCode PULSE_ADMIN_V1 = new ResourceOperationCode(
        ResourceConstants.PULSE_ADMIN_V1, OP_PULSE_ADMIN_V1);
	    
    public static final ResourceOperationCode DATA_READ = new ResourceOperationCode(ResourceConstants.DATA_READ,
        OP_DATA_READ,
	    		new ResourceOperationCode[]{
          LIST_DS,
          PULSE_DASHBOARD
    });

    public static final ResourceOperationCode DATA_WRITE = new ResourceOperationCode(ResourceConstants.DATA_WRITE,
        OP_DATA_WRITE,
        new ResourceOperationCode[]{
          DATA_READ,
          QUERY,
          BECOME_LOCK_GRANTOR,
          PUT,
          REMOVE,
          EXECUTE_FUNCTION,
          PULSE_DATABROWSER
    });

    public static final ResourceOperationCode MONITOR = new ResourceOperationCode(ResourceConstants.MONITOR,
        OP_MONITOR,
        new ResourceOperationCode[] {
          DATA_READ,
          EXPORT_CONFIG,
          EXPORT_DATA,
          EXPORT_LOGS,
          EXPORT_OFFLINE_DISKSTORE,
          EXPORT_STACKTRACE,
          SHOW_DEADLOCKS,
          SHOW_LOG,
          SHOW_METRICS,
          SHOW_MISSING_DISKSTORES,
          SHOW_SUBSCRIPTION_QUEUE_SIZE
    });

    public static final ResourceOperationCode ADMIN = new ResourceOperationCode(ResourceConstants.ADMIN,
        OP_ADMIN,
        new ResourceOperationCode[] {
          DATA_WRITE,
          MONITOR,
          ALTER_REGION,
          ALTER_RUNTIME,
          BACKUP_DISKSTORE,
          CHANGE_ALERT_LEVEL,
          CLOSE_DURABLE_CLIENT,
          CLOSE_DURABLE_CQ,
          COMPACT_DISKSTORE,
          CONFIGURE_PDX,
          CREATE_AEQ,
          CREATE_DISKSTORE,
          CREATE_GW_RECEIVER,
          CREATE_GW_SENDER,
          CREATE_INDEX,
          CREATE_REGION,
          DEPLOY,
          DESTROY_DISKSTORE,
          DESTROY_FUNCTION,
          DESTROY_INDEX,
          DESTROY_REGION,
          GC,
          GET,
          IMPORT_CONFIG,
          IMPORT_DATA,
          LIST_DS,
          LOAD_BALANCE_GW_SENDER,
          LOCATE_ENTRY,
          NETSTAT,
          PAUSE_GW_SENDER,
          REBALANCE,
          RENAME_PDX,
          RESUME_GW_SENDER,
          REVOKE_MISSING_DISKSTORE,
          SHUTDOWN,
          STOP_GW_RECEIVER,
          STOP_GW_SENDER,
          UNDEPLOY,
          BACKUP_MEMBERS,
          ROLL_DISKSTORE,
          FORCE_COMPACTION,
          FORCE_ROLL,
          FLUSH_DISKSTORE,
          START_GW_RECEIVER,
          START_GW_SENDER,
          START_MANAGER,
          STOP_MANAGER,
          CREATE_MANAGER,
          STOP_CONTINUOUS_QUERY,
          SET_DISK_USAGE,
	    			PULSE_WEBGFSH, 
	    			PULSE_ADMIN_V1
	    		});
		
	    
	    private final String name;
    private final int opCode;
    private final List<ResourceOperationCode> children;
	    
    private ResourceOperationCode(String name, int opCode) {
	      this.name = name;
	      this.opCode = opCode;
	      VALUES[opCode] = this;
	      OperationNameMap.put(name, this);
	      this.children = null;
	    }
	    
    private ResourceOperationCode(String name, int opCode, ResourceOperationCode[] children) {
		      this.name = name;
		      this.opCode = opCode;
		      VALUES[opCode] = this;
		      OperationNameMap.put(name, this);
      this.children = new ArrayList<ResourceOperationCode>();
      for(ResourceOperationCode code : children) {
        this.children.add(code);
      }
		}
	    
    public List<ResourceOperationCode> getChildren() {
      return children != null ? Collections.unmodifiableList(children) : null;
    }

    public void addChild(ResourceOperationCode code) {
      this.children.add(code);
      }

      /**
     * Returns the <code>OperationCode</code> represented by specified int.
	     */
    public static ResourceOperationCode fromOrdinal(int opCode) {
	      return VALUES[opCode];
	    }

	    /**
	     * Returns the <code>OperationCode</code> represented by specified string.
	     */
	    public static ResourceOperationCode parse(String operationName) {
      return OperationNameMap.get(operationName);
	    }

	    /**
     * Returns the int representing this operation code.
	     * 
     * @return a int representing this operation.
	     */
    public int toOrdinal() {
	      return this.opCode;
	    }

	    /**
	     * Returns a string representation for this operation.
	     * 
	     * @return the name of this operation.
	     */
	    @Override
	    final public String toString() {
	      return this.name;
	    }

	    /**
	     * Indicates whether other object is same as this one.
	     * 
	     * @return true if other object is same as this one.
	     */
	    @Override
	    final public boolean equals(final Object obj) {
	      if (obj == this) {
	        return true;
	      }
	      if (!(obj instanceof ResourceOperationCode)) {
	        return false;
	      }
	      final ResourceOperationCode other = (ResourceOperationCode)obj;
	      return (other.opCode == this.opCode);
	    }

	    /**
	     * Indicates whether other <code>OperationCode</code> is same as this one.
	     * 
	     * @return true if other <code>OperationCode</code> is same as this one.
	     */
	    final public boolean equals(final ResourceOperationCode opCode) {
	      return (opCode != null && opCode.opCode == this.opCode);
	    }

	    /**
	     * Returns a hash code value for this <code>OperationCode</code> which is
     * the same as the int representing its operation type.
	     * 
	     * @return the hashCode of this operation.
	     */
	    @Override
	    final public int hashCode() {
	      return this.opCode;
	    }

    /**
     * Returns true if passed operation is same or any one of its
     * children
     *
     * @param op
     * @return true if  <code>OperationCode</code> matches
     */
    public boolean allowedOp(ResourceOperationCode op) {
      if(this.equals(op))
        return true;
      else {
        if(children!=null) {
          for(ResourceOperationCode child : children) {
            if(child.allowedOp(op))
              return true;
	 }
        }
      }
      return false;
    }
  }

	 public abstract ResourceOperationCode getResourceOperationCode();

	@Override
  public boolean isClientUpdate() {
    return false;
  }

	@Override
	public boolean isPostOperation() {
    return isPostOperation;
	}

  public void setPostOperationResult(Object result) {
    this.isPostOperation = true;
    this.opResult = result;
}

  public Object getOperationResult() {
    return this.opResult;
  }

}