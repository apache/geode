package com.gemstone.gemfire.internal.tools.gfsh.app.cache.data;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;

/**
 * GenericMessage is a light weight message class for holding (key, value) paired
 * data. It holds all primitive values and nested GenericMessage objects. 
 * @author dpark
 *
 */
public class GenericMessage implements GenericMap, Cloneable
{
	private static final long serialVersionUID = 1L;
	
	/**
     * Used to dump messages.
     */
    private final static StringBuffer spaces = new StringBuffer("                               ");
    private static int CHUNK_SIZE_IN_BYTES = 100000;
    
    private ArrayList entryList = new ArrayList(10);


    /**
     * Creates an empty ObjectMessage object.
     */
    public GenericMessage()
    {
    }

    /**
     * Appends a GenericData object to the message.
     * @param key  The key identifying the value. Note that this key needs
     *              not be unique.
     * @param value The value associated with the specified key.
     */
    public GenericMap.Entry add(String key, GenericMap value)
    {
        if (key == null) {
            return null;
        }
        return addEntry(new GenericMap.Entry(key, value, GenericMap.Entry.TYPE_GENERIC_DATA));
    }
    
    public GenericMap.Entry add(String key, Mappable value)
    {
        if (key == null) {
            return null;
        }
        return addEntry(new GenericMap.Entry(key, value, GenericMap.Entry.TYPE_MAPPABLE));
    }

    /**
     * Appends a String to the message.
     * @param key  The key identifying the value. Note that this key needs
     *              not be unique.
     * @param value The value associated with the specified key.
     */
    public GenericMap.Entry add(String key, String value)
    {
        if (key == null) {
            return null;
        }
        return addEntry(new GenericMap.Entry(key, value, GenericMap.Entry.TYPE_STRING));
    }

    /**
     * Appends a boolean value to the message.
     * @param key  The key identifying the value. Note that this key needs
     *              not be unique.
     * @param value The value associated with the specified key.
     */
    public GenericMap.Entry add(String key, boolean value)
    {
        if (key == null) {
            return null;
        }
        return addEntry(new GenericMap.Entry(key, Boolean.valueOf(value), GenericMap.Entry.TYPE_BOOLEAN));
    }

    /**
     * Appends a byte value to the message.
     * @param key  The key identifying the value. Note that this key needs
     *              not be unique.
     * @param value The value associated with the specified key.
     */
    public GenericMap.Entry add(String key, byte value)
    {
        if (key == null) {
            return null;
        }
        return addEntry(new GenericMap.Entry(key, Byte.valueOf(value), GenericMap.Entry.TYPE_BYTE));
    }

    /**
     * Appends a short value to the message.
     * @param key  The key identifying the value. Note that this key needs
     *              not be unique.
     * @param value The value associated with the specified key.
     */
    public GenericMap.Entry add(String key, short value)
    {
        if (key == null) {
            return null;
        }
        return addEntry(new GenericMap.Entry(key, Short.valueOf(value), GenericMap.Entry.TYPE_SHORT));
    }

    /**
     * Appends a int value to the message.
     * @param key  The key identifying the value. Note that this key needs
     *              not be unique.
     * @param value The value associated with the specified key.
     */
    public GenericMap.Entry add(String key, int value)
    {
        if (key == null) {
            return null;
        }
        return addEntry(new GenericMap.Entry(key, Integer.valueOf(value), GenericMap.Entry.TYPE_INTEGER));
    }

    /**
     * Appends a long value to the message.
     * @param key  The key identifying the value. Note that this key needs
     *              not be unique.
     * @param value The value associated with the specified key.
     */
    public GenericMap.Entry add(String key, long value)
    {
        if (key == null) {
            return null;
        }
        return addEntry(new GenericMap.Entry(key, Long.valueOf(value), GenericMap.Entry.TYPE_LONG));
    }

    /**
     * Appends a float value to the message.
     * @param key  The key identifying the value. Note that this key needs
     *              not be unique.
     * @param value The value associated with the specified key.
     */
    public GenericMap.Entry add(String key, float value)
    {
        if (key == null) {
            return null;
        }
        return addEntry(new GenericMap.Entry(key, new Float(value), GenericMap.Entry.TYPE_FLOAT));
    }

    /**
     * Appends a double value to the message.
     * @param key  The key identifying the value. Note that this key needs
     *              not be unique.
     * @param value The value associated with the specified key.
     */
    public GenericMap.Entry add(String key, double value)
    {
        if (key == null) {
            return null;
        }
        return addEntry(new GenericMap.Entry(key, new Double(value), GenericMap.Entry.TYPE_DOUBLE));
    }

    /**
     * Appends an GenericMap.Entry object to the message.
     * @param entry The entry associated with the specified key.
     */
    public GenericMap.Entry addEntry(Entry entry)
    {
        if (entry == null) {
            return null;
        }
        entryList.add(entry);
        return entry;
    }

    /**
     * Returns the Entry object located at the specified index.
     * @param index The index.
     */
    public GenericMap.Entry getEntry(int index)
    {
        if (index < 0 || index >= entryList.size()) {
            return null;
        }
        return (Entry)entryList.get(index);
    }

    /**
     * Returns the first Entry object identified by the specified key found
     * in the message. There may be multiple Entry objects associated with
     * the same key.
     */
    public GenericMap.Entry getEntry(String key)
    {
        if (key == null) {
            return null;
        }
        GenericMap.Entry array[] = (Entry[])entryList.toArray(new GenericMap.Entry[0]);
        for (int i = 0; i < array.length; i++) {
            if (array[i].getKey().equals(key)) {
                return array[i];
            }
        }
        return null;
    }

    /**
     * Returns the first value identified by the specified key found in
     * the message. There may be multiple Entry objects associated with
     * the same key.
     * @param key  The key identifying the interested value.
     */
    public Object getValue(String key)
    {
        GenericMap.Entry entry = getEntry(key);
        if (entry == null) {
            return null;
        }
        return entry.getValue();
    }

    public boolean getBoolean(String key) throws NoSuchFieldException, InvalidTypeException
    {
        GenericMap.Entry entry = getEntry(key);
        if (entry == null) {
            throw new NoSuchFieldException("The field " + key + " is not found.");
        }
        Object val = entry.getValue();
        if (val == null) {
            throw new NullPointerException("The field " + key + " has null value.");
        }
        if (entry.getType() != GenericMap.Entry.TYPE_BOOLEAN) {
            if (val instanceof Boolean == false) {
                throw new InvalidTypeException("The field " + key + " has the type " + val.getClass().getName());
            }
        }
        return ((Boolean)val).booleanValue();
    }

    public byte getByte(String key) throws NoSuchFieldException, InvalidTypeException
    {
        GenericMap.Entry entry = getEntry(key);
        if (entry == null) {
            throw new NoSuchFieldException("The field " + key + " is not found.");
        }
        Object val = entry.getValue();
        if (val == null) {
            throw new NullPointerException("The field " + key + " has null value.");
        }
        if (entry.getType() != GenericMap.Entry.TYPE_BYTE) {
            if (val instanceof Byte == false) {
                throw new InvalidTypeException("The field " + key + " has the type " + val.getClass().getName());
            }
        }
        return ((Byte)val).byteValue();
    }

    public char getChar(String key) throws NoSuchFieldException, InvalidTypeException
    {
        GenericMap.Entry entry = getEntry(key);
        if (entry == null) {
            throw new NoSuchFieldException("The field " + key + " is not found.");
        }
        Object val = entry.getValue();
        if (val == null) {
            throw new NullPointerException("The field " + key + " has null value.");
        }
        if (entry.getType() != GenericMap.Entry.TYPE_CHAR) {
            if (val instanceof Character == false) {
                throw new InvalidTypeException("The field " + key + " has the type " + val.getClass().getName());
            }
        }
        return ((Character)val).charValue();
    }

    public short getShort(String key) throws NoSuchFieldException, InvalidTypeException
    {
        GenericMap.Entry entry = getEntry(key);
        if (entry == null) {
            throw new NoSuchFieldException("The field " + key + " is not found.");
        }
        Object val = entry.getValue();
        if (val == null) {
            throw new NullPointerException("The field " + key + " has null value.");
        }
        if (entry.getType() != GenericMap.Entry.TYPE_SHORT) {
            if (val instanceof Short == false) {
                throw new InvalidTypeException("The field " + key + " has the type " + val.getClass().getName());
            }
        }
        return ((Short)val).shortValue();
    }

    public int getInt(String key) throws NoSuchFieldException, InvalidTypeException
    {
        GenericMap.Entry entry = getEntry(key);
        if (entry == null) {
            throw new NoSuchFieldException("The field " + key + " is not found.");
        }
        Object val = entry.getValue();
        if (val == null) {
            throw new NullPointerException("The field " + key + " has null value.");
        }
        if (entry.getType() != GenericMap.Entry.TYPE_INTEGER) {
            if (val instanceof Integer == false) {
                throw new InvalidTypeException("The field " + key + " has the type " + val.getClass().getName());
            }
        }
        return ((Integer)val).intValue();
    }

    public long getLong(String key) throws NoSuchFieldException, InvalidTypeException
    {
        GenericMap.Entry entry = getEntry(key);
        if (entry == null) {
            throw new NoSuchFieldException("The field " + key + " is not found.");
        }
        Object val = entry.getValue();
        if (val == null) {
            throw new NullPointerException("The field " + key + " has null value.");
        }
        if (entry.getType() != GenericMap.Entry.TYPE_LONG) {
            if (val instanceof Long == false) {
                throw new InvalidTypeException("The field " + key + " has the type " + val.getClass().getName());
            }
        }
        return ((Long)val).longValue();
    }

    public float getFloat(String key) throws NoSuchFieldException, InvalidTypeException
    {
        GenericMap.Entry entry = getEntry(key);
        if (entry == null) {
            throw new NoSuchFieldException("The field " + key + " is not found.");
        }
        Object val = entry.getValue();
        if (val == null) {
            throw new NullPointerException("The field " + key + " has null value.");
        }
        if (entry.getType() != GenericMap.Entry.TYPE_LONG) {
            if (val instanceof Long == false) {
                throw new InvalidTypeException("The field " + key + " has the type " + val.getClass().getName());
            }
        }
        return ((Long)val).longValue();
    }

    public double getDouble(String key) throws NoSuchFieldException, InvalidTypeException
    {
        GenericMap.Entry entry = getEntry(key);
        if (entry == null) {
            throw new NoSuchFieldException("The field " + key + " is not found.");
        }
        Object val = entry.getValue();
        if (val == null) {
            throw new NullPointerException("The field " + key + " has null value.");
        }
        if (entry.getType() != GenericMap.Entry.TYPE_DOUBLE) {
            if (val instanceof Double == false) {
                throw new InvalidTypeException("The field " + key + " has the type " + val.getClass().getName());
            }
        }
        return ((Double)val).doubleValue();
    }

    public String getString(String key) throws NoSuchFieldException, InvalidTypeException
    {
        GenericMap.Entry entry = getEntry(key);
        if (entry == null) {
            throw new NoSuchFieldException("The field " + key + " is not found.");
        }
        Object val = entry.getValue();
        if (entry.getType() != GenericMap.Entry.TYPE_STRING) {
            if (val instanceof String == false) {
                throw new InvalidTypeException("The field " + key + " has the type " + val.getClass().getName());
            }
        }
        return (String)val;
    }

    /**
     * Returns the Entry at the specified index position. It returns null if the index
     * is out of range.
     */
    public GenericMap.Entry getEntryAt(int index)
    {
        if (index < 0 && index >= size()) {
            return null;
        }
        return (Entry)entryList.get(index);
    }

    /**
     * Returns the value found at the specified index position. Ir returns null if
     * the index is out of range.
     */
    public Object getValueAt(int index)
    {
        GenericMap.Entry entry = getEntryAt(index);
        if (entry == null) {
            return null;
        }
        return entry.getValue();
    }

    /**
     * Returns the key of the entry found at the specified index position. It returns null
     * if the index is out of range.
     */
    public String getNameAt(int index)
    {
        GenericMap.Entry entry = getEntryAt(index);
        if (entry == null) {
            return null;
        }
        return entry.getKey();
    }

    /**
     * Returns the index of the first Entry that matches the specified key.
     * It returns -1 if not found.
     * @param key The Entry key.
     */
    public int indexOf(String key)
    {
        if (key == null) {
            return -1;
        }
        int index = -1;
        GenericMap.Entry array[] = (Entry[])entryList.toArray(new GenericMap.Entry[0]);
        for (int i = 0; i < array.length; i++) {
            if (array[i].getKey().equals(key)) {
                index = i;
                break;
            }
        }
        return index;
    }

    /**
     * Returns the index of the last Entry that matches the specified key.
     * It returns -1 if not found.
     * @param key The Entry key.
     */
    public int lastIndexOf(String key)
    {
        if (key == null) {
            return -1;
        }
        int index = -1;
        GenericMap.Entry array[] = (Entry[])entryList.toArray(new GenericMap.Entry[0]);
        for (int i = array.length - 1; i >= 0; i--) {
            if (array[i].getKey().equals(key)) {
                index = i;
                break;
            }
        }
        return index;
    }

    /**
     * Returns the last Entry found in the message. t returns null if the message
     * does not contain any data.
     */
    public GenericMap.Entry getLastEntry()
    {
        if (entryList.size() > 0) {
            return (Entry)entryList.get(entryList.size()-1);
        } else {
            return null;
        }
    }

    /**
     * Returns the last value found in the message. t returns null if the message
     * does not contain any data.
     */
    public Object getLastValue()
    {
        GenericMap.Entry entry = getLastEntry();
        if (entry == null) {
            return null;
        }
        return entry.getValue();
    }

    /**
     * Returns the first Entry found in the message. It returns null if the message
     * does not contain any data.
     */
    public GenericMap.Entry getFirstEntry()
    {
        if (entryList.size() > 0) {
            return (Entry)entryList.get(0);
        } else {
            return null;
        }
    }

    /**
     * Returns the first value found in the message. It returns null if the message
     * does not contain any data.
     */
    public Object getFirstValue()
    {
        GenericMap.Entry entry = getFirstEntry();
        if (entry == null) {
            return null;
        }
        return entry.getValue();
    }

    /**
     * Returns true if the message contains nested ObjectMessage.
     */
    public boolean hasGenericData()
    {
        GenericMap.Entry data[] = getAllEntries();
        for (int i = 0; i < data.length; i++) {
            if (data[i].getValue() instanceof GenericMap) {
                return true;
            }
        }
        return false;
    }

    /**
     * Removes the specified entry from the message.
     */
    public boolean remove(Entry entry)
    {
        return entryList.remove(entry);
    }

    /**
     * Removes the Entry object at the specified position.
     */
    public GenericMap.Entry remove(int index)
    {
        return (Entry)entryList.remove(index);
    }
    
    public Collection getEntries()
    {
    	return entryList;
    }


    /**
     * Returns the number of Entry objects contained in this message.
     */
    public int size()
    {
        return entryList.size();
    }

    /**
     * Returns all of the Entry objects in the form of array.
     */
    public GenericMap.Entry[] getAllEntries()
    {
        return (Entry[])entryList.toArray(new GenericMap.Entry[0]);
    }

    /**
     * Returns all of the Entry objects that contain the non-ObjectMessage type,
     * i.e., no nested messages.
     */
    public GenericMap.Entry[] getAllPrimitives()
    {
        GenericMap.Entry data[] = getAllEntries();
        GenericMap.Entry messages[] = new GenericMap.Entry[data.length];
        int count = 0;
        for (int i = 0; i < data.length; i++) {
            if (data[i].getValue() instanceof GenericMap == false) {
                messages[count++] = data[i];
            }
        }
        GenericMap.Entry m[] = new GenericMap.Entry[count];
        System.arraycopy(messages, 0, m, 0, count);
        return m;
    }

    /**
     * Returns the number of non-ObjectMessage objects in this message.
     */
    public int getPrimitiveCount()
    {
        GenericMap.Entry data[] = getAllEntries();
//        GenericMap.Entry messages[] = new GenericMap.Entry[data.length]; //FindBugs - unused
        int count = 0;
        for (int i = 0; i < data.length; i++) {
            if (data[i].getValue() instanceof GenericMap == false) {
                count++;
            }
        }
        return count;
    }

    /**
     * Returns all of the Entry objects that contain the ObjectMessage type, i.e., nested
     * messages.
     */
    public GenericMap.Entry[] getAllGenericData()
    {
        GenericMap.Entry data[] = getAllEntries();
        GenericMap.Entry messages[] = new GenericMap.Entry[data.length];
        int count = 0;
        for (int i = 0; i < data.length; i++) {
            if (data[i].getValue() instanceof GenericMap) {
                messages[count++] = data[i];
            }
        }
        GenericMap.Entry m[] = new GenericMap.Entry[count];
        System.arraycopy(messages, 0, m, 0, count);
        return m;
    }

    /**
     * Returns the number of ObjectMessage objects in this message.
     */
    public int getGenericDataCount()
    {
        GenericMap.Entry data[] = getAllEntries();
//        GenericMap.Entry messages[] = new GenericMap.Entry[data.length]; //FindBugs - unused
        int count = 0;
        for (int i = 0; i < data.length; i++) {
            if (data[i].getValue() instanceof GenericMap) {
                count++;
            }
        }
        return count;
    }

    public void clear()
    {
        entryList.clear();
    }

    private void convertToString(StringBuffer buffer, GenericMap message, int level)
    {
        GenericMap.Entry data[] = message.getAllEntries();
        for (int i = 0; i < data.length; i++) {
            if (data[i].getType() == GenericMap.Entry.TYPE_MAPPABLE) {
                buffer.append(spaces.substring(0, level*3) + data[i].getKey() + "*****" + "\n");
                convertToString(buffer, (GenericMap)data[i].getValue(), level+1);
            } else {
                buffer.append(spaces.substring(0, level*3)+ data[i].getKey() + " = " + data[i].getValue() + "\n");
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
    private void dump(PrintWriter writer, GenericMap message, int level)
    {
        GenericMap.Entry data[] = message.getAllEntries();
        for (int i = 0; i < data.length; i++) {
            if (data[i].getType() == GenericMap.Entry.TYPE_MAPPABLE) {
                writer.println(spaces.substring(0, level*3) + data[i].getKey() + "*****");
                dump(writer, (GenericMap)data[i].getValue(), level+1);
            } else {
                writer.println(spaces.substring(0, level*3)+ data[i].getKey() + " = " + data[i].getValue());
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

    private static byte[] serializeDataSerializable(DataSerializable obj) throws IOException
    {
        byte[] ba = null;
        ByteArrayOutputStream baos = new ByteArrayOutputStream(CHUNK_SIZE_IN_BYTES);
        DataOutputStream dos = new DataOutputStream(baos);
        DataSerializer.writeObject(obj, dos);
        dos.flush();
        ba = baos.toByteArray();
        
        return ba;
    }

    private static Object deserializeDataSerializable(byte[] byteArray)
        throws IOException, ClassNotFoundException
    {
        ByteArrayInputStream bais = new ByteArrayInputStream(byteArray);
        DataInputStream dis = new DataInputStream(bais);
        Object obj = DataSerializer.readObject(dis);
        return obj;
    }

    private byte[] serialize(Object obj) throws IOException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(obj);
        oos.flush();
        byte array[] = baos.toByteArray();
        baos.close();
        return array;
    }

    private Object deserialize(byte objArray[]) throws IOException, ClassNotFoundException
    {
        ByteArrayInputStream bais = new ByteArrayInputStream(objArray);
        ObjectInputStream ois = new ObjectInputStream(bais);
        Object obj = ois.readObject();
        return obj;
    }

    /**
     * Returns a shallow copy of this <tt>ObjectMessage</tt> instance.  (The
     * elements themselves are not copied.)
     *
     * @return  a clone of this <tt>ObjectMessage</tt> instance.
     */
    public Object clone()
    {
        GenericMessage dup = new GenericMessage();
        dup.entryList = (ArrayList)this.entryList.clone();
        return dup;
    }

	public void fromData(DataInput dataInput) throws IOException, ClassNotFoundException
	{
		int count = dataInput.readInt();
		Entry entry;
		byte type;
		String key;
		Object value;
		for (int i = 0; i < count; i++) {
			type = dataInput.readByte();
			key = DataSerializer.readString(dataInput);
			value = DataSerializer.readObject(dataInput);
			entry = new GenericMap.Entry(key, value, type);
			entryList.add(entry);
		}
	}

	public void toData(DataOutput dataOutput) throws IOException
	{
		int count = entryList.size();
		dataOutput.writeInt(count);
		Entry entry;
		for (Iterator iterator = entryList.iterator(); iterator.hasNext();) {
			entry = (Entry)iterator.next();
			dataOutput.writeByte(entry.getType());
			DataSerializer.writeString(entry.getKey(), dataOutput);
			DataSerializer.writeObject(entry.getValue(), dataOutput);
		}
	}
}