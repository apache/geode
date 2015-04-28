package com.gemstone.gemfire.internal.tools.gfsh.app.cache.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;

public interface GenericMap extends DataSerializable
{
	public Entry add(String key, GenericMap value);
	public Entry add(String key, Mappable value);
	public Entry add(String key, String value);
	public Entry add(String key, boolean value);
	public Entry add(String key, byte value);
	public Entry add(String key, short value);
	public Entry add(String key, int value);
	public Entry add(String key, long value);
	public Entry add(String key, float value);
	public Entry add(String key, double value);
	public Entry addEntry(Entry entry);
	public Entry getEntry(int index);
	public Entry getEntry(String key);
	public Object getValue(String key);
	public boolean getBoolean(String key) throws NoSuchFieldException, InvalidTypeException;
	public byte getByte(String key) throws NoSuchFieldException, InvalidTypeException;
	public char getChar(String key) throws NoSuchFieldException, InvalidTypeException;
	public short getShort(String key) throws NoSuchFieldException, InvalidTypeException;
	public int getInt(String key) throws NoSuchFieldException, InvalidTypeException;
	public long getLong(String key) throws NoSuchFieldException, InvalidTypeException;
	public float getFloat(String key) throws NoSuchFieldException, InvalidTypeException;
	public double getDouble(String key) throws NoSuchFieldException, InvalidTypeException;
	public String getString(String key) throws NoSuchFieldException, InvalidTypeException;
	public Entry getEntryAt(int index);
	public Object getValueAt(int index);
	public String getNameAt(int index);
	public int indexOf(String key);
	public int lastIndexOf(String key);
	public Entry getLastEntry();
	public Object getLastValue();
	public Entry getFirstEntry();
	public Object getFirstValue();
	public boolean hasGenericData();
	public boolean remove(Entry entry);
	public Entry remove(int index);
	public Collection getEntries();
	public int size();
	public Entry[] getAllEntries();
	public Entry[] getAllPrimitives();
	public int getPrimitiveCount();
	public Entry[] getAllGenericData();
	public int getGenericDataCount();
	public void clear();
	public void dump(OutputStream out);
	public Object clone();
	
	/**
	 * Entry is an element that contains a (key, value) pair. This
	 * class is exclusively used by GenericMessage.
	 */
	public static class Entry implements DataSerializable
	{
	    /**
	     * The value type is TYPE_OBJECT if it is a non-primitive and non-MapMessage
	     * type.
	     */
	    public final static byte      TYPE_GENERIC_DATA = 1;

	    /**
	     * The value type is TYPE_MAPPABLE if it is Mappable type.
	     */
	    public final static byte      TYPE_MAPPABLE = 2;

	    public final static byte      TYPE_BYTE = 3;
	    public final static byte      TYPE_CHAR = 4;
	    public final static byte      TYPE_DOUBLE = 5;
	    public final static byte      TYPE_FLOAT = 6;
	    public final static byte      TYPE_INTEGER = 7;
	    public final static byte      TYPE_LONG = 8;
	    public final static byte      TYPE_SHORT = 9;
	    public final static byte      TYPE_BOOLEAN = 10;
	    public final static byte      TYPE_STRING = 11;

	    private byte type = TYPE_GENERIC_DATA;
	    private String key;
	    private Object value;

	    public Entry()
	    {
	    }
	    
	    /**
	     * Creates a new Entry object.
	     * @param key  The key identifying the value.
	     * @param value The value.
	     * @param type  The value type.
	     */
	    public Entry(String key, Object value, byte type)
	    {
	        this.key = key;
	        this.value = value;
	        this.type = type;
	    }

	    /**
	     * Creates a new Entry object. The value type is set to the default
	     * type TYPE_GENERIC_DATA.
	     * @param key  The key identifying the value.
	     * @param value The value.
	     */
	    public Entry(String key, GenericMap value)
	    {
	        this(key, value, TYPE_GENERIC_DATA);
	    }

	    /**
	     * Returns the key identifying the value.
	     */
	    public String getKey()
	    {
	        return key;
	    }

	    /**
	     * Returns the value.
	     */
	    public Object getValue()
	    {
	        return value;
	    }

	    /**
	     * Returns the value type.
	     */
	    public short getType()
	    {
	        return type;
	    }
	    
	    public int intValue() throws InvalidTypeException
	    {
	    	if (type == TYPE_INTEGER) {
	    		return ((Integer)value).intValue();
	    	} else if (type == TYPE_LONG) {
	    		return ((Long)value).intValue();
	    	} else if (type == TYPE_BYTE) {
	    		return ((Byte)value).intValue();
	    	} else if (type == TYPE_CHAR) {
	    		return Character.getNumericValue(((Character)value).charValue());
	    	} else if (type == TYPE_SHORT) {
	    		return ((Short)value).intValue();
	    	} else if (type == TYPE_FLOAT) {
	    		return ((Float)value).intValue();
	    	} else if (type == TYPE_DOUBLE) {
	    		return ((Double)value).intValue();
	    	} else if (type == TYPE_BOOLEAN) {
	    		if (((Boolean)value).booleanValue()) {
	    			return 1;
	    		} else {
	    			return 0;
	    		}
	    	} else {
	    		throw new InvalidTypeException(value.getClass() + ": Unable to convert object to int.");
	    	}
	    }
	    
	    public boolean isPrimitive()
	    {
	    	return type == TYPE_BYTE || 
	    			type == TYPE_CHAR ||
	    			type == TYPE_DOUBLE ||
	    			type == TYPE_FLOAT ||
	    			type == TYPE_INTEGER ||
	    			type == TYPE_LONG ||
	    			type == TYPE_SHORT ||
	    			type == TYPE_BOOLEAN;
	    }

		public void fromData(DataInput dataInput) throws IOException, ClassNotFoundException
		{
			type = dataInput.readByte();
			key = DataSerializer.readString(dataInput);
			value = DataSerializer.readObject(dataInput);
		}

		public void toData(DataOutput dataOutput) throws IOException
		{
			dataOutput.writeByte(type);
			DataSerializer.writeString(key, dataOutput);
			DataSerializer.writeObject(value, dataOutput);
		}
	}
}
