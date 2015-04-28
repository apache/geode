package com.gemstone.gemfire.internal.tools.gfsh.app.cache.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.internal.InternalDataSerializer;

/**
 * ListMapMessage is a light weight map that guarantees the original order of
 * keys as they are added to the map. It holds all primitive values and 
 * nested ListMapMessage objects. 
 * @author dpark
 *
 */
public class ListMapMessage implements Mappable, Cloneable
{
	private static final long serialVersionUID = 1L;

	/**
     * Used to dump messages.
     */
    private final static StringBuffer spaces = new StringBuffer("                               ");

    private ListMap map = new ListMap();

    /**
     * Creates an empty ListMapMessage object.
     */
    public ListMapMessage()
    {
    }

    /**
     * Puts a Mappable to the message.
     * @param name  The unique name identifying the value.
     * @param mappable The value associated with the specified name.
     */
    public void put(String name, Mappable mappable)
    {
        map.put(name, mappable);
    }
    
    /**
     * Puts a Listable to the message.
     * @param name  The unique name identifying the value.
     * @param listable The value associated with the specified name.
     */
    public void put(String name, Listable listable)
    {
        map.put(name, listable);
    }


    /**
     * Puts a String to the message.
     * @param name  The unique name identifying the value.
     * @param value The value associated with the specified name.
     */
    public void put(String name, String value)
    {
    	map.put(name, value);
    }

    /**
     * Appends a boolean value to the message.
     * @param name  The unique name identifying the value.
     * @param value The value associated with the specified name.
     */
    public void put(String name, boolean value)
    {
    	map.put(name, Boolean.valueOf(value));
    }

    /**
     * Puts a byte value to the message.
     * @param name  The unique name identifying the value.
     * @param value The value associated with the specified name.
     */
    public void put(String name, byte value)
    {
        map.put(name, Byte.valueOf(value));
    }

    /**
     * Appends a short value to the message.
     * @param name  The unique name identifying the value.
     * @param value The value associated with the specified name.
     */
    public void put(String name, short value)
    {
        map.put(name, Short.valueOf(value));
    }

    /**
     * Puts a int value to the message.
     * @param name  The unique name identifying the value.
     * @param value The value associated with the specified name.
     */
    public void put(String name, int value)
    {
        map.put(name, Integer.valueOf(value));
    }

    /**
     * Appends a long value to the message.
     * @param name  The unique name identifying the value.
     * @param value The value associated with the specified name.
     */
    public void put(String name, long value)
    {
        map.put(name, Long.valueOf(value));
    }

    /**
     * Puts a float value to the message.
     * @param name  The unique name identifying the value.
     * @param value The value associated with the specified name.
     */
    public void put(String name, float value)
    {
        map.put(name, new Float(value));
    }

    /**
     * Puts a double value to the message.
     ** @param name  The unique name identifying the value.
     * @param value The value associated with the specified name.
     */
    public void put(String name, double value)
    {
        map.put(name, new Double(value));
    }

    /**
     * Returns the object identified by the specified name found
     * in the message.
     */
    public Object getValue(String name)
    {
        return map.get(name);
    }

    /**
     * Returns the boolean value identified by the specified name found
     * in the message.
     * @param name  The unique name identifying the value.
     * @throws NoSuchFieldException Thrown if the mapping value is not found.
     * @throws InvalidTypeException Thrown if the value type is different.
     */
    public boolean getBoolean(String name) throws NoSuchFieldException, InvalidTypeException
    {
    	Object value = map.get(name);
    	if (value == null) {
    		throw new NoSuchFieldException("The field " + name + " is not found.");
    	} else {
    		if (value instanceof Boolean) {
    			return ((Boolean)value).booleanValue();
    		} else {
    			throw new InvalidTypeException("The field " + name + " has the type " + value.getClass().getName());
    		}
    	}
    }
    
    /**
     * Returns the byte value identified by the specified name found
     * in the message.
     * @param name  The unique name identifying the value.
     * @throws NoSuchFieldException Thrown if the mapping value is not found.
     * @throws InvalidTypeException Thrown if the value type is different.
     */
    public byte getByte(String name) throws NoSuchFieldException, InvalidTypeException
    {
    	Object value = map.get(name);
    	if (value == null) {
    		throw new NoSuchFieldException("The field " + name + " is not found.");
    	} else {
    		if (value instanceof Byte) {
    			return ((Byte)value).byteValue();
    		} else {
    			throw new InvalidTypeException("The field " + name + " has the type " + value.getClass().getName());
    		}
    	}
    }

    /**
     * Returns the char value identified by the specified name found
     * in the message.
     * @param name  The unique name identifying the value.
     * @throws NoSuchFieldException Thrown if the mapping value is not found.
     * @throws InvalidTypeException Thrown if the value type is different.
     */
    public char getChar(String name) throws NoSuchFieldException, InvalidTypeException
    {
    	Object value = map.get(name);
    	if (value == null) {
    		throw new NoSuchFieldException("The field " + name + " is not found.");
    	} else {
    		if (value instanceof Character) {
    			return ((Character)value).charValue();
    		} else {
    			throw new InvalidTypeException("The field " + name + " has the type " + value.getClass().getName());
    		}
    	}
    }

    /**
     * Returns the short value identified by the specified name found
     * in the message.
     * @param name  The unique name identifying the value.
     * @throws NoSuchFieldException Thrown if the mapping value is not found.
     * @throws InvalidTypeException Thrown if the value type is different.
     */
    public short getShort(String name) throws NoSuchFieldException, InvalidTypeException
    {
    	Object value = map.get(name);
    	if (value == null) {
    		throw new NoSuchFieldException("The field " + name + " is not found.");
    	} else {
    		if (value instanceof Short) {
    			return ((Short)value).shortValue();
    		} else {
    			throw new InvalidTypeException("The field " + name + " has the type " + value.getClass().getName());
    		}
    	}
    }

    /**
     * Returns the int value identified by the specified name found
     * in the message.
     * @param name  The unique name identifying the value.
     * @throws NoSuchFieldException Thrown if the mapping value is not found.
     * @throws InvalidTypeException Thrown if the value type is different.
     */
    public int getInt(String name) throws NoSuchFieldException, InvalidTypeException
    {
    	Object value = map.get(name);
    	if (value == null) {
    		throw new NoSuchFieldException("The field " + name + " is not found.");
    	} else {
    		if (value instanceof Integer) {
    			return ((Integer)value).intValue();
    		} else {
    			throw new InvalidTypeException("The field " + name + " has the type " + value.getClass().getName());
    		}
    	}
    }

    /**
     * Returns the long value identified by the specified name found
     * in the message.
     * @param name  The unique name identifying the value.
     * @throws NoSuchFieldException Thrown if the mapping value is not found.
     * @throws InvalidTypeException Thrown if the value type is different.
     */
    public long getLong(String name) throws NoSuchFieldException, InvalidTypeException
    {
    	Object value = map.get(name);
    	if (value == null) {
    		throw new NoSuchFieldException("The field " + name + " is not found.");
    	} else {
    		if (value instanceof Long) {
    			return ((Long)value).longValue();
    		} else {
    			throw new InvalidTypeException("The field " + name + " has the type " + value.getClass().getName());
    		}
    	}
    }

    /**
     * Returns the float value identified by the specified name found
     * in the message.
     * @param name  The unique name identifying the value.
     * @throws NoSuchFieldException Thrown if the mapping value is not found.
     * @throws InvalidTypeException Thrown if the value type is different.
     */
    public float getFloat(String name) throws NoSuchFieldException, InvalidTypeException
    {
    	Object value = map.get(name);
    	if (value == null) {
    		throw new NoSuchFieldException("The field " + name + " is not found.");
    	} else {
    		if (value instanceof Float) {
    			return ((Float)value).floatValue();
    		} else {
    			throw new InvalidTypeException("The field " + name + " has the type " + value.getClass().getName());
    		}
    	}
    }

    /**
     * Returns the double value identified by the specified name found
     * in the message.
     * @param name  The unique name identifying the value.
     * @throws NoSuchFieldException Thrown if the mapping value is not found.
     * @throws InvalidTypeException Thrown if the value type is different.
     */
    public double getDouble(String name) throws NoSuchFieldException, InvalidTypeException
    {
    	Object value = map.get(name);
    	if (value == null) {
    		throw new NoSuchFieldException("The field " + name + " is not found.");
    	} else {
    		if (value instanceof Double) {
    			return ((Double)value).doubleValue();
    		} else {
    			throw new InvalidTypeException("The field " + name + " has the type " + value.getClass().getName());
    		}
    	}
    }
    
    /**
     * Returns the String value identified by the specified name found
     * in the message.
     * @param name  The unique name identifying the value.
     * @throws NoSuchFieldException Thrown if the mapping value is not found.
     * @throws InvalidTypeException Thrown if the value type is different.
     */
    public String getString(String name) throws NoSuchFieldException, InvalidTypeException
    {
    	Object value = map.get(name);
    	if (value == null) {
    		throw new NoSuchFieldException("The field " + name + " is not found.");
    	} else {
    		if (value instanceof String) {
    			return (String)value;
    		} else {
    			throw new InvalidTypeException("The field " + name + " has the type " + value.getClass().getName());
    		}
    	}
    }

    /**
     * Returns true if the message contains nested Mappable.
     */
    public boolean hasMappable()
    {
        Map.Entry entries[] = getAllEntries();
        for (int i = 0; i < entries.length; i++) {
            if (entries[i].getValue() instanceof Mappable) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Returns true if the message contains nested Listable.
     */
    public boolean hasListable()
    {
        Map.Entry entries[] = getAllEntries();
        for (int i = 0; i < entries.length; i++) {
            if (entries[i].getValue() instanceof Listable) {
                return true;
            }
        }
        return false;
    }

    /**
     * Removes the specified entry from the message.
     */
    public Object remove(String name)
    {
        return map.remove(name);
    }

    /**
     * Returns the number of entries in this message.
     */
    public int size()
    {
        return map.size();
    }
    
    public Collection values()
    {
    	return map.values();
    }
    
    public Collection getValues()
    {
    	return map.values();
    }
    
    public Set keys()
    {
    	return map.keySet();
    }
    
    public Set getKeys()
    {
    	return map.keySet();
    }
    
    public Set getEntries()
    {
    	return map.entrySet();
    }

    /**
     * Returns all of the entries in the form of array.
     */
    public Map.Entry[] getAllEntries()
    {
        return (Map.Entry[])map.entrySet().toArray(new Map.Entry[0]);
    }

    /**
     * Returns all of the primitive entries in the message.
     */
    public Map.Entry[] getAllPrimitives()
    {
    	Map.Entry entries[] = getAllEntries();
    	Map.Entry messages[] = new Map.Entry[entries.length];
        int count = 0;
        for (int i = 0; i < entries.length; i++) {
            if (entries[i].getValue() instanceof Mappable == false) {
                messages[count++] = entries[i];
            }
        }
        Map.Entry m[] = new Map.Entry[count];
        System.arraycopy(messages, 0, m, 0, count);
        return m;
    }

    /**
     * Returns the number primitive entries in this message.
     */
    public int getPrimitiveCount()
    {
        Map.Entry entries[] = getAllEntries();
        int count = 0;
        for (int i = 0; i < entries.length; i++) {
            if (entries[i].getValue() instanceof Mappable == false) {
                count++;
            }
        }
        return count;
    }

    /**
     * Returns all of the entries that have the ListMapMessage type, i.e., nested
     * messages.
     */
    public Map.Entry[] getAllMappables()
    {
    	Map.Entry entries[] = getAllEntries();
    	Map.Entry messages[] = new Map.Entry[entries.length];
        int count = 0;
        for (int i = 0; i < entries.length; i++) {
            if (entries[i].getValue() instanceof Mappable) {
                messages[count++] = entries[i];
            }
        }
        Map.Entry m[] = new Map.Entry[count];
        System.arraycopy(messages, 0, m, 0, count);
        return m;
    }
    
    /**
     * Returns all of the entries that have the ListMapMessage type, i.e., nested
     * messages.
     */
    public Map.Entry[] getAllListables()
    {
    	Map.Entry entries[] = getAllEntries();
    	Map.Entry messages[] = new Map.Entry[entries.length];
        int count = 0;
        for (int i = 0; i < entries.length; i++) {
            if (entries[i].getValue() instanceof Listable) {
                messages[count++] = entries[i];
            }
        }
        Map.Entry m[] = new Map.Entry[count];
        System.arraycopy(messages, 0, m, 0, count);
        return m;
    }

    /**
     * Returns the number of Mappable entries in this message.
     */
    public int getMappableCount()
    {
    	Map.Entry entries[] = getAllEntries();
        int count = 0;
        for (int i = 0; i < entries.length; i++) {
            if (entries[i].getValue() instanceof Mappable) {
                count++;
            }
        }
        return count;
    }
    
    /**
     * Returns the number of Listable entries in this message.
     */
    public int getListableCount()
    {
    	Map.Entry entries[] = getAllEntries();
        int count = 0;
        for (int i = 0; i < entries.length; i++) {
            if (entries[i].getValue() instanceof Listable) {
                count++;
            }
        }
        return count;
    }

    /**
     * Clears the message. It removes all of the entries in the message.
     *
     */
    public void clear()
    {
        map.clear();
    }

    private void convertToString(StringBuffer buffer, Mappable message, int level)
    {
    	Map.Entry entries[] = message.getAllEntries();
        for (int i = 0; i < entries.length; i++) {
            if (entries[i].getValue() instanceof Mappable) {
                buffer.append(spaces.substring(0, level*3) + entries[i].getKey() + "*****" + "\n");
                convertToString(buffer, (Mappable)entries[i].getValue(), level+1);
            } else {
                buffer.append(spaces.substring(0, level*3)+ entries[i].getKey() + " = " + entries[i].getValue() + "\n");
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
    private void dump(PrintWriter writer, Mappable message, int level)
    {
    	Map.Entry entries[] = message.getAllEntries();
        for (int i = 0; i < entries.length; i++) {
        	if (entries[i].getValue() instanceof Mappable) {
                writer.println(spaces.substring(0, level*3) + entries[i].getKey() + "*****");
                dump(writer, (Mappable)entries[i].getValue(), level+1);
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
     * Returns a shallow copy of this <tt>ListMapMessage</tt> instance.  (The
     * elements themselves are not copied.)
     *
     * @return  a clone of this <tt>ListMapMessage</tt> instance.
     */
    public Object clone()
    {
        ListMapMessage dup = new ListMapMessage();
        dup.map = (ListMap)this.map.clone();
        return dup;
    }

	public void fromData(DataInput dataInput) throws IOException, ClassNotFoundException
	{
	  InternalDataSerializer.invokeFromData(map, dataInput);
	}

	public void toData(DataOutput dataOutput) throws IOException
	{
	  InternalDataSerializer.invokeToData(map, dataOutput);
	}
}