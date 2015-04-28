package com.gemstone.gemfire.internal.tools.gfsh.app.cache.data;

import java.io.OutputStream;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.DataSerializable;

public interface Mappable extends DataSerializable
{
	public void put(String key, Mappable mappable);
	public void put(String key, Listable listable);
	public void put(String key, String value);
	public void put(String key, boolean value);
	public void put(String key, byte value);
	public void put(String key, short value);
	public void put(String key, int value);
	public void put(String key, long value);
	public void put(String key, float value);
	public void put(String key, double value);
	public Object getValue(String key);
	public boolean getBoolean(String key) throws NoSuchFieldException, InvalidTypeException;
	public byte getByte(String name) throws NoSuchFieldException, InvalidTypeException;
	public char getChar(String key) throws NoSuchFieldException, InvalidTypeException;
	public short getShort(String key) throws NoSuchFieldException, InvalidTypeException;
	public int getInt(String key) throws NoSuchFieldException, InvalidTypeException;
	public long getLong(String key) throws NoSuchFieldException, InvalidTypeException;
	public float getFloat(String key) throws NoSuchFieldException, InvalidTypeException;
	public double getDouble(String key) throws NoSuchFieldException, InvalidTypeException;
	public String getString(String key) throws NoSuchFieldException, InvalidTypeException;
	public boolean hasMappable();
	public boolean hasListable();
	public Object remove(String key);
	public int size();
	public Collection getValues();
	public Set getKeys();
	public Set getEntries();
	public Map.Entry[] getAllEntries();
	public Map.Entry[] getAllPrimitives();
	public int getPrimitiveCount();
	public Map.Entry[] getAllMappables();
	public Map.Entry[] getAllListables();
	public int getMappableCount();
	public int getListableCount();
	public void clear();
	public void dump(OutputStream out);
	public Object clone();
	
}
