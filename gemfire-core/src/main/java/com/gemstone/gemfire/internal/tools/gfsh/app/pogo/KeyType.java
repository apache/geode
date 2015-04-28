package com.gemstone.gemfire.internal.tools.gfsh.app.pogo;

import java.util.Set;

/**
 * KeyType represents the schema definitions for predefining keys for
 * lightweight self-describing message classes such as MapLite provided
 * in this package. 
 * @author dpark
 *
 */
public interface KeyType
{
	/**
	 * Return the universal ID that uniquely represents the key type across
	 * space and time. The underlying message class implementation must 
	 * guarantee the uniqueness of this ID to properly marshal objects 
	 * crossing network and language boundaries. This ID is static and
	 * permanent for the life of the key type class. 
	 */
	public Object getId();
	
	/**
	 * Returns the version number.
	 */
	public int getVersion();
	
	/**
	 * Returns the key count.
	 */
	public int getKeyCount();
	
	/**
	 * Returns the index of the key.
	 */
	public int getIndex();
	
	/**
	 * Returns the name of the key.
	 */
	public String getName();
	
	/**
	 * Returns the class of the key.
	 */
	public Class getType();
	
	/**
	 * Returns the entire keys.
	 */
	public KeyType[] getValues();
	
	/**
	 * Returns the entire keys of the specified version.
	 * @param version The version number.
	 */
	public KeyType[] getValues(int version);

	/**
	 * Returns the key of the specified key name.
	 * @param name The key name.
	 */
	public KeyType getKeyType(String name);
	
	/**
	 * Returns true if delta propagation is enabled.
	 */
	public boolean isDeltaEnabled();
	
	/**
	 * Returns true if the key value is to be kept serialized until
	 * it is accessed. This applies per key instance.
	 */
	public boolean isKeyKeepSerialized();
	
	/**
	 * Returns true if the network payload is to be compressed.
	 */
	public boolean isCompressionEnabled();
	
	/**
	 * Returns true if any of the key values is to be kept serialized.
	 */
	public boolean isPayloadKeepSerialized();
	
	/**
	 * Returns the key name set.
	 */
	public Set<String> getNameSet();
	
	/**
	 * Returns true if the specified key is defined.
	 * @param name The key to check.
	 */
	public boolean containsKey(String name);
}
