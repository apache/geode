package com.gemstone.gemfire.management.internal.security;

import java.util.HashMap;
import java.util.Map;

import com.gemstone.gemfire.cache.operations.OperationContext;

public abstract class ResourceOperationContext extends OperationContext {
	
	 public static class ResourceOperationCode {
		 
		private static final byte OP_LIST_DS = 1;	
	    private static final byte OP_READ_DS = 2;
	    private static final byte OP_SET_DS = 3;
	    private static final byte OP_ADMIN_DS = 4;
	    private static final byte OP_CHANGE_ALERT_LEVEL_DS = 5;
	    private static final byte OP_BACKUP_DS = 6;
	    private static final byte OP_REMOVE_DISKSTORE_DS = 7;
	    private static final byte OP_SHUTDOWN_DS = 8;
	    private static final byte OP_QUERYDATA_DS = 9;
	    private static final byte OP_REBALANCE_DS = 10;
	    
	    private static final byte OP_EXPORT_DATA_REGION = 11;
	    private static final byte OP_IMPORT_DATA_REGION = 12 ;
	    private static final byte OP_PUT_REGION = 13;
	    private static final byte OP_LOCATE_ENTRY_REGION = 14;
	    
	    private static final byte OP_PULSE_DASHBOARD = 15;
	    private static final byte OP_PULSE_DATABROWSER = 16;
	    private static final byte OP_PULSE_WEBGFSH = 17;
	    private static final byte OP_PULSE_ADMIN_V1 = 18;
	    
	    
	    private static final ResourceOperationCode[] VALUES = new ResourceOperationCode[20];
	    private static final Map OperationNameMap = new HashMap();
	    
	    public static final ResourceOperationCode LIST_DS = new ResourceOperationCode(ResourceConstants.LIST_DS, OP_LIST_DS);
	    public static final ResourceOperationCode READ_DS = new ResourceOperationCode(ResourceConstants.READ_DS, OP_READ_DS);
	    public static final ResourceOperationCode SET_DS = new ResourceOperationCode(ResourceConstants.SET_DS, OP_SET_DS);

	    public static final ResourceOperationCode CHANGE_ALERT_LEVEL_DS = new ResourceOperationCode(ResourceConstants.CHANGE_ALERT_LEVEL_DS, OP_CHANGE_ALERT_LEVEL_DS);
	    public static final ResourceOperationCode BACKUP_DS = new ResourceOperationCode(ResourceConstants.BACKUP_DS, OP_BACKUP_DS);
	    public static final ResourceOperationCode REMOVE_DISKSTORE_DS = new ResourceOperationCode(ResourceConstants.REMOVE_DISKSTORE_DS, OP_REMOVE_DISKSTORE_DS);
	    public static final ResourceOperationCode SHUTDOWN_DS = new ResourceOperationCode(ResourceConstants.SHUTDOWN_DS, OP_SHUTDOWN_DS);
	    public static final ResourceOperationCode QUERYDATA_DS = new ResourceOperationCode(ResourceConstants.QUERYDATA_DS, OP_QUERYDATA_DS);
	    public static final ResourceOperationCode REBALANCE_DS = new ResourceOperationCode(ResourceConstants.REBALANCE, OP_REBALANCE_DS);
	    
	    public static final ResourceOperationCode EXPORT_DATA_REGION = new ResourceOperationCode(ResourceConstants.EXPORT_DATA, OP_EXPORT_DATA_REGION);
	    public static final ResourceOperationCode IMPORT_DATA_REGION = new ResourceOperationCode(ResourceConstants.IMPORT_DATA, OP_IMPORT_DATA_REGION);
	    public static final ResourceOperationCode PUT_REGION = new ResourceOperationCode(ResourceConstants.PUT, OP_PUT_REGION);
	    public static final ResourceOperationCode LOCATE_ENTRY_REGION = new ResourceOperationCode(ResourceConstants.LOCATE_ENTRY, OP_LOCATE_ENTRY_REGION);	    
	    
	    public static final ResourceOperationCode PULSE_DASHBOARD = new ResourceOperationCode(ResourceConstants.PULSE_DASHBOARD, OP_PULSE_DASHBOARD);
	    public static final ResourceOperationCode PULSE_DATABROWSER = new ResourceOperationCode(ResourceConstants.PULSE_DATABROWSER, OP_PULSE_DATABROWSER);
	    public static final ResourceOperationCode PULSE_WEBGFSH = new ResourceOperationCode(ResourceConstants.PULSE_WEBGFSH, OP_PULSE_WEBGFSH);
	    public static final ResourceOperationCode PULSE_ADMIN_V1 = new ResourceOperationCode(ResourceConstants.PULSE_ADMIN_V1, OP_PULSE_ADMIN_V1);
	    
	    public static final ResourceOperationCode ADMIN_DS = new ResourceOperationCode(ResourceConstants.ADMIN_DS, OP_ADMIN_DS,
	    		new ResourceOperationCode[]{
	          CHANGE_ALERT_LEVEL_DS, 
	          BACKUP_DS, 
	          REMOVE_DISKSTORE_DS, 
	          SHUTDOWN_DS, 
	          QUERYDATA_DS, 
	    			REBALANCE_DS, 
	    			PULSE_DASHBOARD, 
	    			PULSE_DATABROWSER, 
	    			PULSE_WEBGFSH, 
	    			PULSE_ADMIN_V1
	    		});
		
	    
	    private final String name;
	    private final byte opCode;
	    private final ResourceOperationCode[] children;
	    
	    private ResourceOperationCode(String name, byte opCode) {
	      this.name = name;
	      this.opCode = opCode;
	      VALUES[opCode] = this;
	      OperationNameMap.put(name, this);
	      this.children = null;
	    }
	    
	    private ResourceOperationCode(String name, byte opCode, ResourceOperationCode[] children) {
		      this.name = name;
		      this.opCode = opCode;
		      VALUES[opCode] = this;
		      OperationNameMap.put(name, this);
		      this.children = children;
		}
	    
	    
	    
	    public ResourceOperationCode[] getChildren() {
        return children;
      }

      /**
	     * Returns the <code>OperationCode</code> represented by specified byte.
	     */
	    public static ResourceOperationCode fromOrdinal(byte opCode) {
	      return VALUES[opCode];
	    }

	    /**
	     * Returns the <code>OperationCode</code> represented by specified string.
	     */
	    public static ResourceOperationCode parse(String operationName) {
	      return (ResourceOperationCode)OperationNameMap.get(operationName);
	    }

	    /**
	     * Returns the byte representing this operation code.
	     * 
	     * @return a byte representing this operation.
	     */
	    public byte toOrdinal() {
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
	     * the same as the byte representing its operation type.
	     * 
	     * @return the hashCode of this operation.
	     */
	    @Override
	    final public int hashCode() {
	      return this.opCode;
	    }

	    
	 }
	

	 public abstract ResourceOperationCode getResourceOperationCode();
	 
	 /*
	@Override
	public OperationCode getOperationCode() {
		// TODO Auto-generated method stub
		return null;
	}*/

	@Override
	public boolean isPostOperation() {
		return false;
	}

}
