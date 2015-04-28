package com.gemstone.gemfire.internal.tools.gfsh.app.cache.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;

/**
 * An implementation of a Map that guarantees original order of keys as they
 * are added to the Map.
 */
public class ListMap extends AbstractMap implements Cloneable, DataSerializable
{
	/**
	 * Version id for Serializable 
	 */
	private static final long serialVersionUID = 1L;
	
	protected ListSet entrySet = new ListSet();

	/**
	 * Internal class to implement a Set that uses an ArrayList to keep
	 * original order of keys
	 */
	static protected class ListSet extends AbstractSet implements Serializable
	{
		protected ArrayList entryList = new ArrayList();

		public ListSet()
		{
			super();
		}

		public Iterator iterator()
		{
			return entryList.iterator();
		}

		public int size()
		{
			return entryList.size();
		}

		public boolean add(Object o)
		{
			boolean retVal = false;
			if (!entryList.contains(o)) {
				retVal = entryList.add(o);
			}
			return retVal;
		}

		/**
		 * Internal method to put the entry by looking for existing entry and *
		 * returning it or add new entry.
		 */
		public Object putEntry(Object entry)
		{
			Object retVal = entry;
			int index = entryList.indexOf(entry);
			if (index >= 0) {
				retVal = entryList.get(index);
			} else {
				entryList.add(entry);
			}
			return retVal;
		}
	}

	/**
	 * Internal class to implement the Map.Entry interface that holds the
	 * entry objects in the Map.
	 */
	static protected class ListEntry implements Entry, Serializable
	{
		protected Object key = null;

		protected Object value = null;

		public ListEntry(Object pKey)
		{
			key = pKey;
		}

		public Object getKey()
		{
			return key;
		}

		public Object getValue()
		{
			return value;
		}

		public Object setValue(Object pValue)
		{
			Object prevValue = value;
			value = pValue;
			return prevValue;
		}

		public boolean equals(Object o)
		{
			boolean retVal = false;
			Object otherKey = o;
			if (o instanceof ListEntry) {
				otherKey = ((ListEntry) o).getKey();
			}
			retVal = (key == null && otherKey == null)
					|| (key != null && key.equals(otherKey));
			return retVal;
		}

		public int hashCode()
		{
			return (key != null) ? key.hashCode() : 0;
		}
	}

	/** 
	 * Default constructor 
	 */
	public ListMap()
	{
		super();
	}

	/**
	 * Implement put to allow this Map to be writable. If the key represents a
	 * new element in the Map, then it is added to the end of the Map.
	 * Otherwise, the existing entry is updated with the new value. 
	 * 
	 * @param key
	 *            the key of the value to set. 
	 * @param value
	 *            the value to set. 
	 * @return the previous value set at this key (null indicates new or
	 *         previous value was null).
	 */
	public Object put(Object key, Object value)
	{
		Map.Entry entry = new ListEntry(key);
		entry = (Map.Entry)entrySet.putEntry(entry);
		return entry.setValue(value);
	}

	/** 
	 * Return the Set that contains a list of Map.Entry objects for this Map. 
	 */
	public Set entrySet()
	{
		return entrySet;
	}
	
	public Object clone() 
	{
		ListMap result = null;
		try {
			result = (ListMap)super.clone();
		} catch (Exception e) {
			// ignore
		}
		return result;
    }

	public void fromData(DataInput dataInput) throws IOException, ClassNotFoundException 
	{
		int size = dataInput.readInt();
		String key;
		Object value;
		for (int i = 0; i < size; i++) {
			key = DataSerializer.readString(dataInput);
			value = DataSerializer.readObject(dataInput);
			put(key, value);
		}
	}

	public void toData(DataOutput dataOutput) throws IOException 
	{
		dataOutput.writeInt(entrySet.size());
		Map.Entry entry;
		for (Iterator iterator = entrySet.iterator(); iterator.hasNext(); ) {
			entry = (Map.Entry)iterator.next();
			DataSerializer.writeObject(entry.getKey(), dataOutput);
			DataSerializer.writeObject(entry.getValue(), dataOutput);
		}
	}
}