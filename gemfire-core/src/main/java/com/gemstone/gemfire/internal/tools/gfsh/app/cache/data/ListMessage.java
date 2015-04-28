package com.gemstone.gemfire.internal.tools.gfsh.app.cache.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import com.gemstone.gemfire.DataSerializer;

/**
 * ListMessage is a light weight message class for holding only values
 * (no keys). It holds all primitive values and nested ListMessage objects. 
 * @author dpark
 *
 */
public class ListMessage implements Listable, Cloneable
{
	private static final long serialVersionUID = 1L;

	/**
     * Used to dump messages.
     */
    private final static StringBuffer spaces = new StringBuffer("                               ");

    private ArrayList entryList = new ArrayList(10);

    /**
     * Creates an empty ListMessage object.
     */
    public ListMessage()
    {
    }

    /**
     * Puts a Listable to the message.
     * @param listable The nested Listable value to append.
     */
    public void add(Listable listable)
    {
        entryList.add(listable);
    }
    
    /**
     * Puts a Mappable to the message.
     * @param mappable The nested Mappable value to append.
     */
    public void add(Mappable mappable)
    {
        entryList.add(mappable);
    }

    /**
     * Puts a String to the message.
     * @param value The String value to append.
     */
    public void add(String value)
    {
    	entryList.add(value);
    }

    /**
     * Appends a boolean value to the message.
     * @param value The boolean value to append.
     */
    public void add(boolean value)
    {
    	entryList.add(Boolean.valueOf(value));
    }

    /**
     * Appends a byte value to the message.
     * @param value The byte value to append.
     */
    public void add(byte value)
    {
    	entryList.add(Byte.valueOf(value));
    }

    /**
     * Appends a short value to the message.
     * @param value The short value to append.
     */
    public void add(short value)
    {
    	entryList.add(Short.valueOf(value));
    }

    /**
     * Appends a int value to the message.
     * @param value The int value to append.
     */
    public void add(int value)
    {
    	entryList.add(Integer.valueOf(value));
    }

    /**
     * Appends a long value to the message.
     * @param value The long value to append.
     */
    public void add(long value)
    {
    	entryList.add(Long.valueOf(value));
    }

    /**
     * Puts a float value to the message.
     * @param value The float value to append.
     */
    public void add(float value)
    {
    	entryList.add(new Float(value));
    }

    /**
     * Puts a double value to the message.
     * @param value The double value to append.
     */
    public void add(double value)
    {
    	entryList.add(new Double(value));
    }

    /**
     * Returns the element at the specified position in this list.
     *
     * @param  index index of element to return.
     * @return the element at the specified position in this list.
     * @throws    IndexOutOfBoundsException if index is out of range <tt>(index
     * 		  &lt; 0 || index &gt;= size())</tt>.
     */
    public Object getValue(int index) throws IndexOutOfBoundsException
    {
    	return entryList.get(index);
    }

    /**
     * Returns the element at the specified position in this list.
     *
     * @param  index index of element to return.
     * @return the element at the specified position in this list.
     * @throws    IndexOutOfBoundsException if index is out of range <tt>(index
     * 		  &lt; 0 || index &gt;= size())</tt>.
     * @throws InvalidTypeException Thrown if the value type is different.
     */
    public boolean getBoolean(int index) throws IndexOutOfBoundsException, InvalidTypeException
    {
    	Object value = getValue(index);
		if (value instanceof Boolean) {
			return ((Boolean)value).booleanValue();
		} else {
			throw new InvalidTypeException("The value at index " + index + " has the type " + value.getClass().getName());
		}
    }
    
    /**
     * Returns the element at the specified position in this list.
     *
     * @param  index index of element to return.
     * @return the element at the specified position in this list.
     * @throws    IndexOutOfBoundsException if index is out of range <tt>(index
     * 		  &lt; 0 || index &gt;= size())</tt>.
     * @throws InvalidTypeException Thrown if the value type is different.
     */
    public byte getByte(int index) throws IndexOutOfBoundsException, InvalidTypeException
    {
    	Object value = getValue(index);
		if (value instanceof Byte) {
			return ((Byte)value).byteValue();
		} else {
			throw new InvalidTypeException("The value at index " + index + " has the type " + value.getClass().getName());
		}
    }

    /**
     * Returns the element at the specified position in this list.
     *
     * @param  index index of element to return.
     * @return the element at the specified position in this list.
     * @throws    IndexOutOfBoundsException if index is out of range <tt>(index
     * 		  &lt; 0 || index &gt;= size())</tt>.
     * @throws InvalidTypeException Thrown if the value type is different.
     */
    public char getChar(int index) throws IndexOutOfBoundsException, InvalidTypeException
    {
    	Object value = getValue(index);
		if (value instanceof Character) {
			return ((Character)value).charValue();
		} else {
			throw new InvalidTypeException("The value at index " + index + " has the type " + value.getClass().getName());
		}
    }

    /**
     * Returns the element at the specified position in this list.
     *
     * @param  index index of element to return.
     * @return the element at the specified position in this list.
     * @throws    IndexOutOfBoundsException if index is out of range <tt>(index
     * 		  &lt; 0 || index &gt;= size())</tt>.
     * @throws InvalidTypeException Thrown if the value type is different.
     */
    public short getShort(int index) throws IndexOutOfBoundsException, InvalidTypeException
    {
    	Object value = getValue(index);
		if (value instanceof Short) {
			return ((Short)value).shortValue();
		} else {
			throw new InvalidTypeException("The value at index " + index + " has the type " + value.getClass().getName());
		}
    }

    /**
     * Returns the element at the specified position in this list.
     *
     * @param  index index of element to return.
     * @return the element at the specified position in this list.
     * @throws    IndexOutOfBoundsException if index is out of range <tt>(index
     * 		  &lt; 0 || index &gt;= size())</tt>.
     * @throws InvalidTypeException Thrown if the value type is different.
     */
    public int getInt(int index) throws IndexOutOfBoundsException, InvalidTypeException
    {
    	Object value = getValue(index);
		if (value instanceof Integer) {
			return ((Integer)value).intValue();
		} else {
			throw new InvalidTypeException("The value at index " + index + " has the type " + value.getClass().getName());
		}
    }

    /**
     * Returns the element at the specified position in this list.
     *
     * @param  index index of element to return.
     * @return the element at the specified position in this list.
     * @throws    IndexOutOfBoundsException if index is out of range <tt>(index
     * 		  &lt; 0 || index &gt;= size())</tt>.
     * @throws InvalidTypeException Thrown if the value type is different.
     */
    public long getLong(int index) throws IndexOutOfBoundsException, InvalidTypeException
    {
    	Object value = getValue(index);
		if (value instanceof Long) {
			return ((Long)value).intValue();
		} else {
			throw new InvalidTypeException("The value at index " + index + " has the type " + value.getClass().getName());
		}
    }
    
    /**
     * Returns the element at the specified position in this list.
     *
     * @param  index index of element to return.
     * @return the element at the specified position in this list.
     * @throws    IndexOutOfBoundsException if index is out of range <tt>(index
     * 		  &lt; 0 || index &gt;= size())</tt>.
     * @throws InvalidTypeException Thrown if the value type is different.
     */
    public float getFloat(int index) throws IndexOutOfBoundsException, InvalidTypeException
    {
    	Object value = getValue(index);
		if (value instanceof Float) {
			return ((Float)value).intValue();
		} else {
			throw new InvalidTypeException("The value at index " + index + " has the type " + value.getClass().getName());
		}
    }

    /**
     * Returns the element at the specified position in this list.
     *
     * @param  index index of element to return.
     * @return the element at the specified position in this list.
     * @throws    IndexOutOfBoundsException if index is out of range <tt>(index
     * 		  &lt; 0 || index &gt;= size())</tt>.
     * @throws InvalidTypeException Thrown if the value type is different.
     */
    public double getDouble(int index) throws IndexOutOfBoundsException, InvalidTypeException
    {
    	Object value = getValue(index);
		if (value instanceof Double) {
			return ((Double)value).intValue();
		} else {
			throw new InvalidTypeException("The value at index " + index + " has the type " + value.getClass().getName());
		}
    }
    
    /**
     * Returns the element at the specified position in this list.
     *
     * @param  index index of element to return.
     * @return the element at the specified position in this list.
     * @throws    IndexOutOfBoundsException if index is out of range <tt>(index
     * 		  &lt; 0 || index &gt;= size())</tt>.
     * @throws InvalidTypeException Thrown if the value type is different.
     */
    public String getString(int index) throws IndexOutOfBoundsException, InvalidTypeException
    {
    	Object value = getValue(index);
		if (value instanceof String) {
			return (String)value;
		} else {
			throw new InvalidTypeException("The value at index " + index + " has the type " + value.getClass().getName());
		}
    }

    /**
     * Returns true if the message contains nested Listable.
     */
    public boolean hasListable()
    {
    	Iterator iterator = entryList.iterator();
    	while (iterator.hasNext()) {
    		if (iterator.next() instanceof Listable) {
    			return true;
    		}
    	}
        return false;
    }

    /**
     * Returns true if the message contains nested Mappable.
     */
    public boolean hasMappable()
    {
    	Iterator iterator = entryList.iterator();
    	while (iterator.hasNext()) {
    		if (iterator.next() instanceof Mappable) {
    			return true;
    		}
    	}
        return false;
    }
    
    /**
     * Removes the specified entry from the message.
     */
    public Object remove(int index)
    {
        return entryList.remove(index);
    }

    /**
     * Returns the number of entries in this message.
     */
    public int size()
    {
        return entryList.size();
    }
    
    public Collection values()
    {
    	return entryList;
    }
    
    public Collection getValues()
    {
    	return entryList;
    }

    /**
     * Returns all of the values in the form of array.
     */
    public Object[] getAllValues()
    {
        return entryList.toArray();
    }

    /**
     * Returns all of the primitive entries in the message.
     */
    public Object[] getAllPrimitives()
    {
    	ArrayList primitiveList = new ArrayList();
    	Iterator iterator = entryList.iterator();
    	Object value;
    	while (iterator.hasNext()) {
    		value = iterator.next();
    		if (value instanceof Listable == false) {
    			primitiveList.add(value);
    		}
    	}
    	return primitiveList.toArray();
    }

    /**
     * Returns the number primitive entries in this message.
     */
    public int getPrimitiveCount()
    {
    	Iterator iterator = entryList.iterator();
    	int count = 0;
    	while (iterator.hasNext()) {
    		if (iterator.next() instanceof Listable == false) {
    			count++;
    		}
    	}
    	return count;
    }

    /**
     * Returns all of the values that have the Listable type, i.e., nested
     * messages.
     */
    public Listable[] getAllListables()
    {
    	ArrayList listMessageList = new ArrayList();
    	Iterator iterator = entryList.iterator();
    	Object value;
    	while (iterator.hasNext()) {
    		value = iterator.next();
    		if (value instanceof Listable) {
    			listMessageList.add(value);
    		}
    	}
    	return (Listable[])listMessageList.toArray(new Listable[0]);
    }
    
    /**
     * Returns all of the values that have the Mappable type, i.e., nested
     * messages.
     */
    public Mappable[] getAllMappables()
    {
    	ArrayList listMessageList = new ArrayList();
    	Iterator iterator = entryList.iterator();
    	Object value;
    	while (iterator.hasNext()) {
    		value = iterator.next();
    		if (value instanceof Mappable) {
    			listMessageList.add(value);
    		}
    	}
    	return (Mappable[])listMessageList.toArray(new Mappable[0]);
    }

    /**
     * Returns the number of Listable entries in this message.
     */
    public int getListableCount()
    {
    	Iterator iterator = entryList.iterator();
    	int count = 0;
    	while (iterator.hasNext()) {
    		if (iterator.next() instanceof Listable) {
    			count++;
    		}
    	}
        return count;
    }
    
    /**
     * Returns the number of Mappable entries in this message.
     */
    public int getMappableCount()
    {
    	Iterator iterator = entryList.iterator();
    	int count = 0;
    	while (iterator.hasNext()) {
    		if (iterator.next() instanceof Mappable) {
    			count++;
    		}
    	}
        return count;
    }

    /**
     * Clears the message. It removes all of the values in the message.
     *
     */
    public void clear()
    {
        entryList.clear();
    }

    private void convertToString(StringBuffer buffer, Listable message, int level)
    {
    	Object values[] = message.getAllValues();
        for (int i = 0; i < values.length; i++) {
            if (values[i] instanceof Listable) {
                buffer.append(spaces.substring(0, level*3) + values[i] + "*****" + "\n");
                convertToString(buffer, (Listable)values[i], level+1);
            } else {
                buffer.append(spaces.substring(0, level*3)+ values[i] + "\n");
            }
        }
    }

//    public void convertToString(StringBuffer buffer)
//    {
//        if (buffer == null) {
//            return;
//        }
//        convertToString(buffer);
//    }

    public String toString()
    {
        StringBuffer buffer = new StringBuffer(100);
        convertToString(buffer, this, 0);
        return buffer.toString();
    }

    /**
     * Recursively dumps the message contents to the specified writer.
     */
    private void dump(PrintWriter writer, Listable message, int level)
    {
    	Object values[] = message.getAllValues();
        for (int i = 0; i < values.length; i++) {
        	if (values[i] instanceof Listable) {
                writer.println(spaces.substring(0, level*3) + values[i] + "*****");
                dump(writer, (Listable)values[i], level+1);
        	} if (values[i] instanceof Listable) {
        		writer.println(spaces.substring(0, level*3) + values[i] + "*****");
                dump(writer, (Mappable)values[i], level+1);
            } else {
                writer.println(spaces.substring(0, level*3)+ values[i]);
            }
        }
    }
    
    /**
     * Recursively dumps the message contents to the specified writer.
     */
    private void dump(PrintWriter writer, Mappable message, int level)
    {
    	Map.Entry entries[] = message.getAllEntries();
        for (int i = 0; i < entries.length; i++) {
        	if (entries[i].getValue() instanceof Mappable) {
                writer.println(spaces.substring(0, level*3) + entries[i].getKey() + "*****");
                dump(writer, (Mappable)entries[i].getValue(), level+1);
        	} else if (entries[i].getValue() instanceof Listable) {
                writer.println(spaces.substring(0, level*3) + entries[i].getKey() + "*****");
                dump(writer, (Listable)entries[i].getValue(), level+1);
            } else {
                writer.println(spaces.substring(0, level*3)+ entries[i].getKey() + " = " + entries[i].getValue());
            }
        }
    }

    /**
     * Dumps the message contents to the specified output stream.
     * @param out   The outputstream to which the contents are dumped.
     */
    public void dump(OutputStream out)
    {
        if (out == null) {
            return;
        }
        PrintWriter writer = new PrintWriter(out);
        dump(writer, this, 0);
        writer.flush();
    }

    /**
     * Dumps the message contents to the standard output stream (System.out).
     */
    public void dump()
    {
       PrintWriter writer = new PrintWriter(System.out);
       dump(writer, this, 0);
       writer.flush();
    }

    /**
     * Returns a shallow copy of this <tt>ListMessage</tt> instance.  (The
     * elements themselves are not copied.)
     *
     * @return  a clone of this <tt>ListMessage</tt> instance.
     */
    public Object clone()
    {
        ListMessage dup = new ListMessage();
        dup.entryList = (ArrayList)this.entryList.clone();
        return dup;
    }

	public void fromData(DataInput dataInput) throws IOException, ClassNotFoundException
	{
		entryList = DataSerializer.readArrayList(dataInput);
	}

	public void toData(DataOutput dataOutput) throws IOException
	{
		DataSerializer.writeArrayList(entryList, dataOutput);
	}
}