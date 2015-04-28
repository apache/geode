package com.gemstone.gemfire.internal.tools.gfsh.app.cache.data;

import java.io.OutputStream;
import java.util.Collection;

import com.gemstone.gemfire.DataSerializable;

public interface Listable extends DataSerializable
{
	public void add(Listable listable);
	public void add(Mappable listable);
	public void add(String value);
	public void add(boolean value);
	public void add(byte value);
	public void add(short value);
	public void add(int value);
	public void add(long value);
	public void add(float value);
	public void add(double value);
	public Object getValue(int index) throws IndexOutOfBoundsException;
	public boolean getBoolean(int index) throws IndexOutOfBoundsException, InvalidTypeException;
	public byte getByte(int index) throws IndexOutOfBoundsException, InvalidTypeException;
	public char getChar(int index) throws IndexOutOfBoundsException, InvalidTypeException;
	public short getShort(int index) throws IndexOutOfBoundsException, InvalidTypeException;
	public int getInt(int index) throws IndexOutOfBoundsException, InvalidTypeException;
	public long getLong(int index) throws IndexOutOfBoundsException, InvalidTypeException;
	public float getFloat(int index) throws IndexOutOfBoundsException, InvalidTypeException;
	public double getDouble(int index) throws IndexOutOfBoundsException, InvalidTypeException;
	public String getString(int index) throws IndexOutOfBoundsException, InvalidTypeException;
	public boolean hasListable();
	public boolean hasMappable();
	public Object remove(int index);
	public int size();
	public Collection getValues();
	public Object[] getAllValues();
	public Object[] getAllPrimitives();
	public int getPrimitiveCount();
	public Listable[] getAllListables();
	public Mappable[] getAllMappables();
	public int getListableCount();
	public int getMappableCount();
	public void clear();
	public void dump(OutputStream out);
	public Object clone();
}
