package com.gemstone.gemfire.internal.tools.gfsh.app.pogo;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.Instantiator;

/**
 * KeyTypeManager manages the key type classes for the process life time.
 * Each key type class requires registration through KeyTypeManager in
 * order to activate them within the process boundary. Using a key type 
 * without registering may fail as the underlying marshaling mechanism is 
 * maybe unable to properly map the key type.
 * 
 * @author dpark
 *
 */
public class KeyTypeManager
{
	
	/**
	 * Map of key types.
	 */
	private static ConcurrentHashMap<UUID, Map<Integer, KeyType>> uuidMap = new ConcurrentHashMap<UUID, Map<Integer, KeyType>>();
	
	/**
	 * Registers the specified key type. It assumes that all of the KeyType
	 * version classes are in the "pogo" sub-package as the specified KeyType.
	 * @param keyTypeName The fully qualified KeyType class name without
	 *                      the version number, i.e., com.foo.orders.FOrder 
	 *                      as opposed to com.foo.orders.pogo.FOrder_v1.
	 */
//FindBugs - private method never called
//	private static void registerKeyType(String keyTypeName)
//	{
//		if (keyTypeName == null) {
//			return;
//		}
//		Class cls;
//		try {
//			cls = Class.forName(keyTypeName);
//			KeyType keyType = (KeyType)cls.newInstance();
//			registerKeyType(keyType);
//		} catch (Exception e) {
//			// ignore
//		}
//	}
	
	/**
	 * Registers the specified key type and all of the previous versions.
	 * A key type must be registered before it can be used by the application.
	 * @param keyType The key type to register.
	 */
	public static void registerKeyType(KeyType keyType)
	{
		if (keyType == null) {
			return;
		}
		
		registerSingleKeyType(keyType);
		
		Package pkg = keyType.getClass().getPackage();
		String keyTypeName;
		if (pkg == null) {
			keyTypeName = "pogo." + keyType.getClass().getSimpleName();
		} else {
			keyTypeName = pkg.getName() + ".pogo." + keyType.getClass().getSimpleName();
		}
		
		int version = keyType.getVersion();
		do {
			try {
				Class cls = Class.forName(keyTypeName + "_v" + version);
				if (cls.isEnum() == false) {
					break;
				}
				Object[] consts = cls.getEnumConstants();
				if (consts != null && consts.length > 0 && consts[0] instanceof KeyType) {
					KeyType ft = (KeyType)consts[0];
					registerSingleKeyType(ft);
				}
			} catch (ClassNotFoundException e) {
				break;
			}
			version--;
			
		} while (true);
	}
	
	/**
	 * Registers the specified key type only. It will not register the 
	 * previous versions.
	 * @param keyType The key type to register
	 */
	public static void registerSingleKeyType(KeyType keyType)
	{
		if (keyType == null) {
			return;
		}
		Map<Integer, KeyType> map = uuidMap.get(keyType.getId());
		if (map == null) {
			map = new TreeMap<Integer, KeyType>();
			uuidMap.put((UUID)keyType.getId(), map);
		}
		map.put(keyType.getVersion(), keyType);
	}
	
	/**
	 * Returns true if the specified key type has been registered previously.
	 * @param keyType The key type to check.
	 */
	public static boolean isRegistered(KeyType keyType)
	{
		if (keyType == null) {
			return false;
		}
		Map<Integer, KeyType> map = uuidMap.get(keyType.getId());
		if (map == null) {
			return false;
		}
		return map.get(keyType.getVersion()) != null;
	}
	
	/**
	 * Loads a new key type. The main key type points to this key
	 * type.
	 * Experimental - NOT USED
	 * @param keyType
	 */
	private static void loadKeyType(KeyType keyType)
	{
		registerKeyType(keyType);
		Package pkg = keyType.getClass().getPackage();
		String keyTypeName = pkg.getName() + keyType.getClass().getSimpleName();
		try {
			Class cls = Class.forName(keyTypeName);
			Field field = cls.getField("VERSION");
			field.setInt(field, keyType.getVersion());
		} catch (ClassNotFoundException e) {
			return;
		} catch (NoSuchFieldException e) {
			return;
		} catch (IllegalAccessException e) {
			return;
		}
	}
	
	/**
	 * Returns the key type of the specified UUID most significant bits,
	 * least significant bits and version.
	 * 
	 * @param uuidMostSigBits The most significant bits.
	 * @param uuidLeastSigBits The least significant bits.
	 * @param version The version number.
	 * @return Returns the key type of the specified UUID most significant
	 *         bits, least significant bits and version. It returns null if 
	 *         the key type is not found.
	 */
	public static KeyType getKeyType(long uuidMostSigBits, long uuidLeastSigBits, int version)
	{
		return getKeyType(new UUID(uuidMostSigBits, uuidLeastSigBits), version);
	}
	
	/**
	 * Returns the key type of the specified UUID and version.
	 * @param uuid The UUID representing the key type.
	 * @param version The version number.
	 * @return Returns the key type of the specified UUID and version. It
	 *         returns null if the key type is not found.
	 */
	public static KeyType getKeyType(UUID uuid, int version)
	{
		Map<Integer, KeyType> map = uuidMap.get(uuid);
		if (map == null) {
			return null;
		}
		return map.get(version);
	}
	
	/**
	 * Returns the entire key type instances of the specified version.
	 * @param keyType The key type.
	 * @param version The version number.
	 * @return Returns the entire key type instances of the specified version.
	 *         It returns null if the key types are not found.
	 */
	public static KeyType[] getValues(KeyType keyType, int version)
	{
		KeyType ft = getKeyType((UUID)keyType.getId(), version);
		if (ft == null) {
			return null;
		}
		return ft.getValues();
	}
}
