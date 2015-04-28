package com.gemstone.gemfire.internal.tools.gfsh.app.pogo;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.Delta;
import com.gemstone.gemfire.InvalidDeltaException;
import com.gemstone.gemfire.internal.HeapDataOutputStream;
import com.gemstone.gemfire.internal.Version;

/**
 * MapLite is a GemFire data class for efficiently storing and retrieving data
 * to/from GemFire data fabrics. It is a lightweight map class designed for
 * delivering self-describing messages over the network without the cost of
 * embedded keys in the wire format. MapLite achieves this by predefining the
 * keys in the form of enum (and String) constants and deploying them as part of
 * the application binary classes. The enum key classes are automatically
 * versioned and generated using the provided IDE plug-in, ensuring full
 * compatibility and coexistence with other versions.
 * <p>
 * In addition to the code generator, MapLite includes the following GemFire
 * cache optimization and operational features while maintaining the same level
 * of self-describing message accessibility.
 * <p>
 * <ul>
 * <li>MapLite is fully POGO compliant.</li>
 * <li>MapLite implements {@link java.util.Map}.</li>
 * <li>MapLite and POGO are in general significantly lighter than POJO and
 * {@link HashMap}. Its wire format is compact and does not require class
 * reflection.</li>
 * <li>MapLite and POGO are faster than POJO and HashMap. Its smaller payload
 * size means it is serialized and delivered faster.</li>
 * <li>MapLite lookup is faster than HashMap. MapLite keeps values internally
 * indexed in an array for faster access.</li>
 * <li>MapLite fully supports GemFire delta propagation.
 * <li>MapLite fully supports selective key inflation (SKI). With SKI, the
 * underlying POGO mechanism inflates only the values that are accessed by the
 * application. The rest of the values are kept deflated until they are
 * accessed. This reduces the memory footprint and eliminates the unnecessary
 * latency overhead introduced by the serialization and deserialization
 * operations.</li>
 * <li>MapLite fully supports the GemFire query service.</li>
 * <li>MapLite is fully integrated with the key class versioning mechanism,
 * which enables multiple versions of MapLite key sets to coexist in the fabric.
 * All versioned key classes are fully forward and backward compatible.</li>
 * <li>MapLite key classes are universally unique across space and time,
 * eradicating the class ID requirement.</li>
 * <li>MapLite is natively integrated with the GemFire command line tool, gfsh.</li>
 * <li>MapLite is language neutral.</li>
 * </ul>
 * <p>
 * <h3>Lighter and Faster</h3>
 * MapLite, in general, is significantly lighter than HashMap in terms of both
 * size and speed. The size of a typical serialized MapLite object is
 * considerably smaller than the counterpart HashMap object. Enum
 * {@link #get(KeyType)} calls are faster than
 * {@link HashMap#get(Object)} because the values are indexed in an
 * array, circumventing the more expensive hash lookup operation.
 * 
 * <h3>Map with enum KeyType Keys</h3>
 * MapLite implements Map and therefore has the same Map methods and behaves
 * exactly like Map. Unlike HashMap which also implements Map, a MapLite object
 * is restricted to a fixed set of predefined keys in an enum class that
 * implements the interface KeyType. This restriction effectively makes MapLite
 * lighter, faster, and more acquiescent than HashMap. It removes the keys from
 * the wire format and provides a valid key list for strict allowed key and type
 * checking.
 * 
 * <h3>Code Generator</h3>
 * Editing keys, although it can be done manually, is done via the provided IDE
 * plug-in which automatically generates a new version of the enum class. The
 * built-in versioning mechanism allows the new versioned enum class to be
 * deployed to the servers and clients during runtime without the requirement of
 * restarting them. The servers automatically load the new versioned class
 * making it immediately available to the application along with the previous
 * versions.
 * 
 * <h3>String Keys</h3>
 * In addition to the enum keys, MapLite also supports String keys. String keys
 * are costlier than enum keys but comparable to HashMap in terms of the put and
 * get speeds. One of the benefits of using String keys is the flexibility of
 * executing ad hoc queries. MapLite is fully compliant with the GemFire query
 * service, making it ideal for object-relational mapping.
 * 
 * <p>
 * <h3>Using MapLite</h3>
 * <ol>
 * <li>
 * Create a <code>{@link KeyType}</code> enum class using the code generator or
 * the provided example template.</li>
 * <li>
 * Register the new <code>KeyType</code> enum class using
 * <code>{@link KeyTypeManager}</code>.</li>
 * <li>
 * Use <code>KeyType</code> to create <code>MapLite</code> objects. Always use
 * {@link #MapLite(KeyType)} to create <code>MapLite</code> objects.</li>
 * <li>
 * Put the MapLite objects into cache regions</li>
 * <li>
 * Get the MapLite objects from cache regions</li>
 * <li>
 * Get values from the objects using <code>KeyType</code> or <code>String</code>
 * keys</li>
 * </ol>
 * 
 * <h3>Examples</h3>
 * 
 * <pre>
 * import com.gemstone.gemfire.internal.tools.gfsh.pogo.KeyTypeManager;
 * import com.gemstone.gemfire.internal.tools.gfsh.pogo.MapLite;
 * import gemfire.examples.model.Dummy;
 * import gemfre.examples.model.pogo.Dummy_v1;
 * import gemfire.examples.model.pogo.Dummy_v2;
 * 
 * . . .
 *  
 * // Register the Dummy key type. This also registers
 * // all of the versions in the sub-package pogo. This registration
 * // call is not is not required if the registration plug-in
 * // is included in the GemFire cache.xml file.
 * KeyTypeManager.registerKeyType(Dummy.getKeyType());
 * 
 * // Create a MapLite object using the latest Dummy version.
 * // Dummy is equivalent to Dummy_v2 assuming that Dummy_v2 is the
 * // latest version.
 * MapLite ml = new MapLite(Dummy.getKeyType());
 * 
 * // put data using the Dummy.Message enum constant
 * ml.put(Dummy.Message, "Hello, world.");
 * 
 * // put data using the string key "Dummy" which is equivalent to
 * // Dummy.Message
 * ml.put("Message", "Hello, world.");
 * 
 * // Get the value using the Dummy.Message enum constant
 * String message = (String) ml.get(Dummy.Message);
 * 
 * // Get the value using the versioned enum class Dummy_v2 which is
 * // equivalent to Dummy assuming Dummy_v2 is the latest version.
 * message = (String) ml.get(Dummy_v2.Message);
 * 
 * // Get the value using the previous versioned class Dummy_v1 which
 * // may or may not work depending on the changes made to version 2.
 * // If the Message key type has been changed then the type cast will
 * // fail.
 * message = (String) ml.get(Dummy_v1.Message);
 * 
 * // Get the value using the string key "Message".
 * message = (String) ml.get("Message");
 * </pre>
 * 
 * @author dpark
 * 
 */
public class MapLite<V> implements DataSerializable, Delta, Map<String, V>, Cloneable
{
	private static final long serialVersionUID = 1L;

	private static final int BIT_MASK_SIZE = 32; // int type

	private KeyType keyType;
	private Object[] values;
	private int keyVersion;
	private int[] dirtyFlags;
	private byte flags;

	// serialized values
	private byte[] serializedBytes;

	/**
	 * <font COLOR="#ff0000"><strong>The use of this constructor is strongly
	 * discouraged. Always use {@link #MapLite(KeyType)} wherever
	 * possible.</strong></font>
	 * <p>
	 * The default constructor creates a new MapLite object with KeyType
	 * undefined. Undefined KeyType may lead to undesired effects. The following
	 * restriction applies when using this constructor:
	 * 
	 * <blockquote><i> {@link #put(KeyType, Object)} or {@link #get(KeyType)}
	 * must be invoked once with a {@link KeyType} enum constant before the
	 * MapLite object can be used. These methods implicitly initialize the
	 * MapLite object with the specified key type. MapLite ignores String keys
	 * until the key type has been assigned. </i></blockquote>
	 * 
	 * An exception to the above restriction is {@link #putAll(Map)} with the
	 * argument type of MapLite. If MapLite is passed in, then putAll()
	 * transforms this <i>empty</i> MapLite object into the passed-in MapLite
	 * key type.
	 * <p>
	 * It is recommended that the overloaded constructor
	 * {@link #MapLite(KeyType)} should always be used wherever possible. This
	 * default constructor is primarily for satisfying Java serialization
	 * restrictions in addition to handling special operations such as putAll().
	 * 
	 */
	public MapLite()
	{
	}

	/**
	 * Creates a new MapLite object with the specified key type. Once created,
	 * all subsequent operations must use the same <code>KeyType</code> enum
	 * class. The key type is obtained from the <i>no-arg</i> static method,
	 * <code>getKeyType()</code>, included in the generated key type class. For
	 * example,
	 * 
	 * <pre>
	 * MapLite ml = new MapLite(Dummy.getKeyType());
	 * </pre>
	 * 
	 * @param keyType
	 *            The key type enum constant to assign to MapLite.
	 */
	public MapLite(KeyType keyType)
	{
		init(keyType);
	}

	/**
	 * Initializes the MapLite object by creating data structures for the
	 * specified key type.
	 * 
	 * @param keyType
	 *            The key type enum constant to assign to MapLite.
	 */
	private void init(KeyType keyType)
	{
		if (keyType == null) {
			return;
		}
		this.keyType = keyType;
		this.keyVersion = keyType.getVersion();
		int count = keyType.getKeyCount();
		this.values = new Object[count];
		int dirtyFlagCount = calculateDirtyFlagCount();
		this.dirtyFlags = new int[dirtyFlagCount];

		if (KeyTypeManager.isRegistered(keyType) == false) {
			KeyTypeManager.registerKeyType(keyType);
		}
	}

	/**
	 * Calculates the dirty flag count. The dirty flags are kept in an array of
	 * integers. Each integer value represents 32 dirty flags.
	 * 
	 * @return Returns the dirty flag count.
	 */
	private int calculateDirtyFlagCount()
	{
		int count = keyType.getKeyCount();
		int dirtyFlagCount = count / BIT_MASK_SIZE;
		int reminder = count % BIT_MASK_SIZE;
		if (reminder > 0) {
			dirtyFlagCount++;
		}
		return dirtyFlagCount;
	}

	/**
	 * Marks all keys dirty.
	 */
	private void dirtyAllKeys()
	{
		if (dirtyFlags != null) {
			for (int i = 0; i < dirtyFlags.length; i++) {
				dirtyFlags[i] = 0xFFFFFFFF;
			}
		}
	}

	/**
	 * Clears the entire dirty flags.
	 */
	private void clearDirty()
	{
		if (dirtyFlags != null) {
			for (int i = 0; i < dirtyFlags.length; i++) {
				dirtyFlags[i] = 0x0;
			}
		}
	}

	/**
	 * Returns the key type constant used to initialize this object.
	 */
	public KeyType getKeyType()
	{
		return keyType;
	}

	/**
	 * Returns the value of the specified key type. If the default constructor
	 * {@link #MapLite()} is used to create this object then this method
	 * implicitly initializes itself with the specified key type if it has not
	 * been initialized previously.
	 * 
	 * @param keyType
	 *            The key type constant to lookup the mapped value.
	 * @return Returns the mapped value. It returns null if the value does not
	 *         exist or it was explicitly set to null.
	 */
	public V get(KeyType keyType)
	{
		if (keyType == null) {
			return null;
		}

		// Initialization is not thread safe.
		// It allows the use of the default constructor but at
		// the expense of the lack of thread safety.
		if (values == null && keyType != null) {
			init(keyType);
		}

		return (V) values[keyType.getIndex()];
	}

	/**
	 * Puts the specified value mapped by the specified key type into this
	 * object. If the default constructor {@link #MapLite()} is used to create
	 * this object then this method implicitly initializes itself with the
	 * specified key type if it has not been initialized previously.
	 * <p>
	 * Note that for MapLite to be language neutral, the value type must 
	 * be a valid POGO type. It must be strictly enforced by the application.
	 * For Java only applications, any Serializable objects are valid.
	 * 
	 * @param keyType
	 *            The key type constant to lookup the mapped value.
	 * @param value
	 *            The value to put into the MapLite object.
	 * @return Returns the old value. It returns null if the old value does not
	 *         exist or has been explicitly set to null.
	 * @throws InvalidKeyException
	 *             A runtime exception thrown if the passed in value type does
	 *             not match the key type.
	 */
	public V put(KeyType keyType, V value) throws InvalidKeyException
	{
		if (keyType == null) {
			return null;
		}

		// Initialization is not thread safe.
		// It allows the use of the default constructor but at
		// the expense of the lack of thread safety.
		if (values == null) {
			init(keyType);
		}
		
		V oldVal = (V) values[keyType.getIndex()];
		values[keyType.getIndex()] = value;
		
		setDirty(keyType, dirtyFlags);
		return oldVal;
	}

	/**
	 * Returns the mapped value for the specified key. It uses the String value
	 * of the key, i.e., key.toString(), to lookup the mapped value.
	 * 
	 * @param key
	 *            The key object.
	 */
	public V get(Object key)
	{
		if (keyType == null || key == null) {
			return null;
		}
		deserialize();
		KeyType keyType = this.keyType.getKeyType(key.toString());
		if (keyType == null) {
			return null;
		}
		return get(keyType);
	}

	/**
	 * Puts the specified value mapped by the specified key into this object.
	 * Unlike {@link #put(KeyType, Object)}, this method will not implicitly
	 * initialize this object if the default constructor is used. If this object
	 * has not bee initialized, then it throws a runtime exception
	 * InvalidKeyException.
	 * 
	 * @param key
	 *            The key object.
	 * @param value
	 *            The value to put into the MapLite object.
	 * @return Returns the old value.
	 */
	public V put(String key, V value) throws InvalidKeyException
	{
		if (keyType == null) {
			if (value == null) {
				throw new InvalidKeyException("KeyType undefined due to the use of the MapLite default constructor. "
						+ "Use MapLite(KeyType) to register the key type.");
			} else {
				throw new InvalidKeyException("Invalid " + value.getClass().getName()
						+ ". KeyType undefined due to the use of the default constructor.");
			}
		}
		deserialize();
		KeyType keyType = this.keyType.getKeyType(key.toString());
		if (keyType == null) {
			return null;
		}
		return put(keyType, value);
	}

	/**
	 * Returns true if GemFire delta propagation is enabled and there are
	 * changes in values.
	 */
	public boolean hasDelta()
	{
		if (keyType.isDeltaEnabled()) {
			return isDirty();
		}
		return false;
	}

	/**
	 * Returns true if there are changes made in values.
	 */
	public boolean isDirty()
	{
		for (int i = 0; i < dirtyFlags.length; i++) {
			if ((dirtyFlags[i] & 0xFFFFFFFF) != 0) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Sets the specified key type dirty.
	 * 
	 * @param keyType
	 *            The key type to set dirty.
	 * @param flags
	 *            The flags that contain the key type.
	 */
	private void setDirty(KeyType keyType, int flags[])
	{
		int index = keyType.getIndex();
		setDirty(index, flags);
	}

	/**
	 * Sets the specified contiguous bit of the flags. A contiguous bit is the
	 * bit number of the contiguous array integers. For example, if the flags
	 * array size is 2 then the contiguous bit of 32 represents the first bit of
	 * the flags[1] integer, 33 represents the second bit, and etc.
	 * 
	 * @param contiguousBit
	 *            The contiguous bit position.
	 * @param flags
	 *            The bit flags.
	 */
	private void setDirty(int contiguousBit, int flags[])
	{
		int dirtyFlagsIndex = contiguousBit / BIT_MASK_SIZE;
		int bit = contiguousBit % BIT_MASK_SIZE;
		flags[dirtyFlagsIndex] |= 1 << bit;
	}

	/**
	 * Returns true if the specified key type is dirty.
	 * 
	 * @param keyType
	 *            The key type to check.
	 * @param flags
	 *            The flags that contain the key type.
	 */
//FindBugs - private method never called
//	private boolean isBitDirty(KeyType keyType, int flags[])
//	{
//		int index = keyType.getIndex();
//		int dirtyFlagsIndex = index / BIT_MASK_SIZE;
//		int bit = index % BIT_MASK_SIZE;
//		return isBitDirty(flags[dirtyFlagsIndex], bit);
//	}

	/**
	 * Returns true if the specified contiguous bit of the flags is set. A
	 * contiguous bit the bit number of the contiguous array integers. For
	 * example, if the flags array size is 2 then the contiguous bit of 32
	 * represents the first bit of the flags[1] integer, 33 represents the
	 * second bit, and etc.
	 * 
	 * @param contiguousBit
	 *            The contiguous bit position
	 * @param flags
	 *            The bit flags
	 */
//FindBugs - private method never called
//	private boolean isDirty(int contiguousBit, int flags[])
//	{
//		int dirtyFlagsIndex = contiguousBit / BIT_MASK_SIZE;
//		int bit = contiguousBit % BIT_MASK_SIZE;
//		return isBitDirty(flags[dirtyFlagsIndex], bit);
//	}

	/**
	 * Returns true if the specified flag bit id dirty.
	 * 
	 * @param flag
	 *            The flag to check.
	 * @param bit
	 *            The bit to compare.
	 * @return true if the specified flag bit id dirty.
	 */
	private boolean isBitDirty(int flag, int bit)
	{
		return ((flag >> bit) & 1) == 1;
	}

	/**
	 * Returns true if the any of the flag bits is dirty.
	 * 
	 * @param flag
	 *            The flag to check.
	 */
	private boolean isDirty(int flag)
	{
		return (flag & 0xFFFFFFFF) != 0;
	}

	/**
	 * Deserializes (inflates) the serialized bytes if has not been done.
	 */
	private void deserialize()
	{
		byte[] byteArray = serializedBytes;
		if (byteArray != null) {
			KeyType[] keyTypeValues = keyType.getValues(keyVersion);
			ByteArrayInputStream bais = new ByteArrayInputStream(byteArray);
			DataInputStream dis = new DataInputStream(bais);
			try {
				for (int i = 0; i < keyTypeValues.length; i++) {
					if (keyTypeValues[i].isKeyKeepSerialized()) {
						// deserialized values
						values[i] = readValue(keyTypeValues, i, dis);
					}
				}
				dis.close();
				serializedBytes = null;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	/**
	 * Reads the value at for the specified key type index.
	 * 
	 * @param keyTypes
	 *            The entire key types that represent the MapLite values.
	 * @param index
	 *            The index of the key to read.
	 * @param input
	 *            The input stream.
	 * @return Returns the read value.
	 * @throws IOException
	 *             Thrown if an IO error encountered.
	 * @throws ClassNotFoundException
	 */
	private Object readValue(KeyType[] keyTypes, int index, DataInput input) throws IOException, ClassNotFoundException
	{
		return MapLiteSerializer.read(keyTypes[index].getType(), input);
	}

	/**
	 * Reads MapLite contents in the specified input stream.
	 * 
	 * @param input
	 *            The input stream.
	 * @throws IOException
	 *             Thrown if an IO error encountered.
	 * @throws ClassNotFoundException
	 *             Thrown if the input stream contains the wrong class type.
	 *             This should never occur with MapLite.
	 */
	public void fromData(DataInput input) throws IOException, ClassNotFoundException
	{
		flags = MapLiteSerializer.readByte(input);
		long mostSigBits = input.readLong();
		long leastSigBits = input.readLong();
		keyVersion = DataSerializer.readUnsignedShort(input);
		keyType = KeyTypeManager.getKeyType(mostSigBits, leastSigBits, keyVersion);
		init(keyType);
		values = new Object[keyType.getKeyCount()];
		KeyType[] keyTypeValues = keyType.getValues(keyVersion);
		if (keyType.isPayloadKeepSerialized()) {
			// need not to lock since fromData is invoked only
			// once by GemFire
			serializedBytes = DataSerializer.readByteArray(input);
			byte[] deserializedBytes = DataSerializer.readByteArray(input);
			ByteArrayInputStream bais = new ByteArrayInputStream(deserializedBytes);
			DataInputStream dis = new DataInputStream(bais);
			for (int i = 0; i < keyTypeValues.length; i++) {
				if (keyTypeValues[i].isKeyKeepSerialized() == false) {
					// deserialized values
					values[i] = readValue(keyTypeValues, i, dis);
				}
			}
			dis.close();
		} else {
			for (int i = 0; i < keyTypeValues.length; i++) {
				values[i] = readValue(keyTypeValues, i, input);
			}
		}
	}

	/**
	 * Writes the value of the specified index to the output stream.
	 * 
	 * @param keyTypes
	 *            The entire key types that represent the MapLite values.
	 * @param index
	 *            The index of the key to write.
	 * @param output
	 *            The output stream.
	 * @throws IOException
	 *             Thrown if an IO error encountered.
	 */
	private void writeValue(KeyType[] keyTypes, int index, DataOutput output) throws IOException
	{
		try {
			MapLiteSerializer.write(keyTypes[index].getType(), values[index], output);
		} catch (Exception ex) {
			throw new InvalidKeyException(ex.getMessage() + keyTypes.getClass() + " index=" + keyTypes[index].getName(), ex);
		}
	}

	/**
	 * Writes the MapLite contents to the specified output stream.
	 * 
	 * @param output
	 *            The output stream.
	 * @throws IOException
	 *             Thrown if an IO error encountered.
	 */
	public void toData(DataOutput output) throws IOException
	{
		MapLiteSerializer.writeByte(flags, output);
		output.writeLong(((UUID) keyType.getId()).getMostSignificantBits());
		output.writeLong(((UUID) keyType.getId()).getLeastSignificantBits());
		DataSerializer.writeUnsignedShort(keyType.getVersion(), output);
		KeyType[] keyTypeValues = keyType.getValues(keyVersion);
		if (keyType.isPayloadKeepSerialized()) {
			// assign byteArray to serializedBytes beforehand to
			// handle race condition
			byte[] byteArray = serializedBytes;
			if (byteArray != null) {
				DataSerializer.writeByteArray(byteArray, output);
				HeapDataOutputStream hdos2 = new HeapDataOutputStream(Version.CURRENT);
				for (int i = 0; i < keyTypeValues.length; i++) {
					if (keyTypeValues[i].isKeyKeepSerialized() == false) {
						// keep it separate in deserialized array.
						// this array is always deserialized
						writeValue(keyTypeValues, i, hdos2);
					}
				}
				DataSerializer.writeByteArray(hdos2.toByteArray(), output);
				hdos2.close();
			} else {
				HeapDataOutputStream hdos = new HeapDataOutputStream(Version.CURRENT);
				HeapDataOutputStream hdos2 = new HeapDataOutputStream(Version.CURRENT);
				for (int i = 0; i < keyTypeValues.length; i++) {
					if (keyTypeValues[i].isKeyKeepSerialized()) {
						// serialize in the normal array
						// the normal array is deserialized only when the
						// one of its keys is accessed.
						writeValue(keyTypeValues, i, hdos);
					} else {
						// keep it separate in deserialized array.
						// this array is always deserialized
						writeValue(keyTypeValues, i, hdos2);
					}
				}
				DataSerializer.writeByteArray(hdos.toByteArray(), output);
				DataSerializer.writeByteArray(hdos2.toByteArray(), output);
				hdos.close();
				hdos2.close();
			}
		} else {
			for (int i = 0; i < keyTypeValues.length; i++) {
				writeValue(keyTypeValues, i, output);
			}
		}
		clearDirty();
	}

	/**
	 * Reads deltas from the specified input stream.
	 * 
	 * @param input
	 *            The input stream.
	 * @throws IOException
	 *             Thrown if an IO error encountered.
	 * @throws InvalidDeltaException
	 *             Thrown if the received deltas cannot be properly applied.
	 */
	public void fromDelta(DataInput input) throws IOException, InvalidDeltaException
	{
		KeyType[] keyTypeValues = keyType.getValues();
		int bitCount = keyTypeValues.length;
		int dirtyFlagCount = dirtyFlags.length;

		int dirtyFlagsToApply[] = new int[dirtyFlagCount];
		for (int i = 0; i < dirtyFlagCount; i++) {
			dirtyFlagsToApply[i] = input.readInt();
			// CacheFactory.getAnyInstance().getLogger().info("dirty = " +
			// dirtyFlagsToApply[i]);
		}

		try {
			int count = BIT_MASK_SIZE; // int
			for (int i = 0; i < dirtyFlagsToApply.length; i++) {
				int dirty = dirtyFlagsToApply[i]; // received dirty
				int userDirty = dirtyFlags[i]; // app dirty
				if (i == dirtyFlagsToApply.length - 1) {
					count = bitCount % BIT_MASK_SIZE;
					if (count == 0 && bitCount != 0) {
						count = BIT_MASK_SIZE;
					}
				}

				// Compare both the current bit and the received bit.
				// The app might be modifying the object. If so, keep the
				// user modified data and discard the change received.
				int startIndex = i * BIT_MASK_SIZE;
				for (int j = 0; j < count; j++) {
					if (isBitDirty(dirty, j)) {
						int index = startIndex + j;
						Object value = MapLiteSerializer.readObject(input);
						// Set the new value only if the app has not set the
						// value
						if (isBitDirty(userDirty, j) == false) {
							values[index] = value;
						}
						// CacheFactory.getAnyInstance().getLogger().info("bit set = "
						// + j + ", index = " + index);
					}
				}
			}
		} catch (ClassNotFoundException ex) {
			// ignore
		}
	}

	/**
	 * Writes deltas to the specified output stream.
	 * 
	 * @param output
	 *            The output stream.
	 * @throws IOException
	 *             Thrown if an IO error encountered.
	 */
	public void toDelta(DataOutput output) throws IOException
	{
		KeyType[] keyTypeValues = keyType.getValues();
		int bitCount = keyTypeValues.length;

		for (int i = 0; i < dirtyFlags.length; i++) {
			output.writeInt(dirtyFlags[i]);
			// System.out.println("dirty = " + dirtyFlags[i]);
		}

		int count = BIT_MASK_SIZE;

		for (int i = 0; i < dirtyFlags.length; i++) {
			int dirty = dirtyFlags[i];
			if (isDirty(dirty) == false) {
				continue;
			}
			if (i == dirtyFlags.length - 1) {
				count = bitCount % BIT_MASK_SIZE;
				if (count == 0 && bitCount != 0) {
					count = BIT_MASK_SIZE;
				}
			}
			int startIndex = i * BIT_MASK_SIZE;
			for (int j = 0; j < count; j++) {
				if (isBitDirty(dirty, j)) {
					int index = startIndex + j;
					MapLiteSerializer.writeObject(values[index], output);
				}
			}
		}
		clearDirty();
	}

	/**
	 * Returns the key type ID that is universally unique. This call is
	 * equivalent to <code>getKeyType().getId()</code>.
	 */
	public Object getId()
	{
		if (keyType == null) {
			return null;
		}
		return keyType.getId();
	}

	/**
	 * Returns the key type version. There are one or more key type versions per
	 * ID. This method call is equivalent to invoking
	 * <code>getKeyType().getVersion()</code>.
	 */
	public int getKeyTypeVersion()
	{
		if (keyType == null) {
			return 0;
		}
		return keyType.getVersion();
	}

	/**
	 * Returns the simple (short) class name of the key type. It returns null if
	 * the key type is not defined.
	 */
	public String getName()
	{
		if (keyType == null) {
			return null;
		}
		return (String) keyType.getClass().getSimpleName();
	}

	/**
	 * Returns the fully qualified class name of the key type. It returns null
	 * if the key type is not defined.
	 */
	public String getKeyTypeName()
	{
		if (keyType == null) {
			return null;
		}
		return (String) keyType.getClass().getSimpleName();
	}

	/**
	 * Returns all of the keys that map non-null values. It returns an empty set
	 * if this object has not been initialized, i.e., KeyType is undefined.
	 */
	public Set<String> keySet()
	{
		TreeSet<String> retSet = new TreeSet<String>();
		if (keyType != null) {

			Set<String> set = (Set<String>) keyType.getNameSet();
			for (String key : set) {
				if (get(key) != null) {
					retSet.add(key);
				}
			}
		}
		return retSet;
	}

	/**
	 * Returns the entire collection of non-null values.
	 */
	public Collection<V> values()
	{
		ArrayList list = new ArrayList(values.length + 1);
		for (int i = 0; i < values.length; i++) {
			if (values[i] != null) {
				list.add(values[i]);
			}
		}
		return Collections.unmodifiableCollection(list);
	}

	/**
	 * Returns the (string key, value) paired entry set that contains only
	 * non-null values.
	 */
	public Set<Map.Entry<String, V>> entrySet()
	{
		if (keyType == null) {
			return null;
		}
		HashMap<String, V> map = new HashMap(keyType.getKeyCount() + 1, 1f);
		for (KeyType ft : keyType.getValues()) {
			Object value = get(ft);
			if (value != null) {
				map.put((String) ft.getName(), get(ft));
			}
		}
		return Collections.unmodifiableMap(map).entrySet();
	}

	/**
	 * Clears the MapLite values. All non-null values are set to null and dirty.
	 */
	public void clear()
	{
		if (values != null) {
			for (int i = 0; i < values.length; i++) {
				if (values[i] != null) {
					setDirty(i, dirtyFlags);
				}
			}
			values = new Object[values.length];
		}
	}

	/**
	 * Returns true if the specified key maps a non-null value. It uses
	 * key.toString() to search the key.
	 * 
	 * @param key
	 *            The key to check.
	 */
	public boolean containsKey(Object key)
	{
		if (keyType == null) {
			return false;
		}
		return get(key) != null;
	}

	/**
	 * Returns true if the specified value exists in this object. It returns
	 * null if the specified value is null and the MapLite object contains one
	 * or more null values.
	 * 
	 * @param value
	 *            The value to search.
	 */
	public boolean containsValue(Object value)
	{
		if (keyType == null) {
			return false;
		}
		for (int i = 0; i < values.length; i++) {
			if (values[i] == value) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Returns true if there are no values stored in this object. A null value
	 * is considered no value.
	 */
	public boolean isEmpty()
	{
		if (keyType == null || values == null) {
			return true;
		}
		for (int i = 0; i < values.length; i++) {
			if (values[i] != null) {
				return false;
			}
		}
		return false;
	}

	/**
	 * Puts all entries found in the specified map into this MapLite object. The
	 * specified map is handled based on its type as follows:
	 * 
	 * <ul>
	 * <li>If the specified map is null then it is ignored.</li>
	 * 
	 * <li>If the specified map is MapLite and this object has not assigned
	 * KeyType then this method shallow-copies the entire map image into this
	 * object. As a result, MapLite object effectively becomes a clone of the
	 * specified map with all of the keys marked dirty.</li>
	 * 
	 * <li>If the specified map is MapLite and this object has the same KeyType
	 * as the map then the above bullet also applies.</li>
	 * 
	 * <li>If the specified map is Map or MapLite with a KeyType that is
	 * different from this object then this method shallow-copies only the valid
	 * keys and values. All invalid keys and values are ignored. The valid keys
	 * must have the same key names defined in this object's KeyType. Similarly,
	 * the valid values must have the same types defined in this object's
	 * KeyType. All valid keys are marked dirty.</li>
	 * </ul>
	 * <p>
	 * Note that the last bullet transforms any Map objects into MapLite
	 * objects.
	 * 
	 * @param map
	 *            Mappings to be stored in this MapLite object. If it is null
	 *            then it is silently ignored.
	 */
	public void putAll(Map<? extends String, ? extends V> map)
	{
		if (map == null) {
			return;
		}

		// if key type is not defined
		if (keyType == null) {
			if (map instanceof MapLite) {
				MapLite ml = (MapLite) map;
				if (ml.getKeyType() == null) {
					return;
				}
				init(ml.getKeyType());
				System.arraycopy(ml.values, 0, values, 0, values.length);
				dirtyAllKeys();
				return;
			} else {
				return;
			}
		}

		// if key type is defined
		if (map instanceof MapLite) {
			MapLite ml = (MapLite) map;
			if (keyType == ml.getKeyType()) {
				System.arraycopy(ml.values, 0, values, 0, values.length);
				dirtyAllKeys();
				return;
			}
		}

		// If Map or MapLite with a different KeyType - key must be string
		Set<? extends Map.Entry<? extends String, ? extends V>> set = map.entrySet();
		for (Map.Entry<? extends String, ? extends V> entry : set) {
			KeyType keyType = this.keyType.getKeyType(entry.getKey());
			if (keyType == null) {
				continue;
			}
			if (entry.getValue() != null && keyType.getType() != entry.getValue().getClass()) {
				continue;
			}
			put(keyType, entry.getValue());
		}
	}

	/**
	 * Removes the specified key's value. This method removes only the value
	 * that the key maps to. The keys are never removed.
	 * 
	 * @param key
	 *            The key of the value to remove.
	 */
	public V remove(Object key)
	{
		if (keyType == null || key == null) {
			return null;
		}

		KeyType keyType = this.keyType.getKeyType(key.toString());
		if (keyType == null) {
			return null;
		}

		V oldVal = (V) values[keyType.getIndex()];

		if (oldVal != null) {
			// if (this.keyType.isDeltaEnabled()) {
			setDirty(keyType, dirtyFlags);
			// }
		}
		// TODO: take care of initial values for primitives
		values[keyType.getIndex()] = null;
		return oldVal;
	}

	/**
	 * Returns the count of the non-null values.
	 */
	public int size()
	{
		if (keyType == null) {
			return 0;
		}
		int count = 0;
		for (int i = 0; i < values.length; i++) {
			if (values[i] != null) {
				count++;
			}
		}
		return count;
	}

	/**
	 * Returns the count of all keys defined by the key type. This method call
	 * is equivalent to invoking
	 * <code>{@link #getKeyType()}.getKeyCount()</code>. It returns 0 if MapLite
	 * is not initialized, i.e., key type is not defined.
	 */
	public int getKeyCount()
	{
		if (keyType == null) {
			return 0;
		}
		return keyType.getKeyCount();
	}

	/**
	 * Clones this object by shallow-copying values. The returned object is the
	 * exact image of this object including deltas and serialized values.
	 */
	public Object clone()
	{
		if (keyType == null) {
			return new MapLite();
		}
		MapLite ml = new MapLite(keyType);
		System.arraycopy(values, 0, ml.values, 0, values.length);
		System.arraycopy(dirtyFlags, 0, ml.dirtyFlags, 0, dirtyFlags.length);
		if (serializedBytes != null) {
			System.arraycopy(serializedBytes, 0, ml.serializedBytes, 0, serializedBytes.length);
		}
		return ml;
	}
}
