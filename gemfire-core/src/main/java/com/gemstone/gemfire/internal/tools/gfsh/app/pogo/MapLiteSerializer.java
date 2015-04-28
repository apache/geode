package com.gemstone.gemfire.internal.tools.gfsh.app.pogo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.DSCODE;
import com.gemstone.gemfire.internal.InternalDataSerializer;

/**
 * MapLiteSerializer serializes MapLite specifics. It is for internal use only.
 * @author dpark
 *
 */
abstract class MapLiteSerializer extends DataSerializer
{
	public static void write(Class cls, Object object, DataOutput out) throws IOException
	{
		if (cls == String.class) {
			writeUTF((String) object, out);
		} else if (cls == boolean.class || cls == Boolean.class) {
			writeBoolean((Boolean) object, out);
		} else if (cls == byte.class || cls == Byte.class) {
			writeByte((Byte) object, out);
		} else if (cls == char.class || cls == Character.class) {
			writeCharacter((Character) object, out);
		} else if (cls == double.class || cls == Double.class) {
			writeDouble((Double) object, out);
		} else if (cls == float.class || cls == Float.class) {
			writeFloat((Float) object, out);
		} else if (cls == int.class || cls == Integer.class) {
			writeInteger((Integer) object, out);
		} else if (cls == long.class || cls == Long.class) {
			writeLong((Long) object, out);
		} else if (cls == short.class || cls == Short.class) {
			writeShort((Short) object, out);
		} else if (cls == MapLite.class) {
			writeMapLite((MapLite) object, out);
		} else if (cls == Date.class) {
			writeObject((Date) object, out);
		} else if (cls == boolean[].class) {
			writeBooleanArray((boolean[]) object, out);
		} else if (cls == byte[].class) {
			writeByteArray((byte[]) object, out);
		} else if (cls == char[].class) {
			writeCharArray((char[]) object, out);
		} else if (cls == double[].class) {
			writeDoubleArray((double[]) object, out);
		} else if (cls == float[].class) {
			writeFloatArray((float[]) object, out);
		} else if (cls == int[].class) {
			writeIntArray((int[]) object, out);
		} else if (cls == long[].class) {
			writeLongArray((long[]) object, out);
		} else if (cls == short[].class) {
			writeShortArray((short[]) object, out);
		} else {
			writeObject(object, out);
		}
	}
	
	public static Object read(Class cls, DataInput in) throws IOException, ClassNotFoundException
	{
		Object value;
		if (cls == String.class) {
			value = readUTF(in);
		} else if (cls == boolean.class || cls == Boolean.class) {
			value = readBoolean(in);
		} else if (cls == byte.class || cls == Byte.class) {
			value = readByte(in);
		} else if (cls == char.class || cls == Character.class) {
			value = readCharacter(in);
		} else if (cls == double.class || cls == Double.class) {
			value = readDouble(in);
		} else if (cls == float.class || cls == Float.class) {
			value = readFloat(in);
		} else if (cls == int.class || cls == Integer.class) {
			value = readInteger(in);
		} else if (cls == long.class || cls == Long.class) {
			value = readLong(in);
		} else if (cls == short.class || cls == Short.class) {
			value = readShort(in);
		} else if (cls == MapLite.class) {
			value = readMapLite(in);
		} else if (cls == Date.class) {
			value = readObject(in);
		} else if (cls == boolean[].class) {
			value = DataSerializer.readBooleanArray(in);
		} else if (cls == byte[].class) {
			value = DataSerializer.readByteArray(in);
		} else if (cls == char[].class) {
			value = DataSerializer.readCharacter(in);
		} else if (cls == double[].class) {
			value = DataSerializer.readByteArray(in);
		} else if (cls == float[].class) {
			value = DataSerializer.readByteArray(in);
		} else if (cls == int[].class) {
			value = DataSerializer.readByteArray(in);
		} else if (cls == long[].class) {
			value = DataSerializer.readByteArray(in);
		} else if (cls == short[].class) {
			value = DataSerializer.readByteArray(in);
		} else {
			value = DataSerializer.readObject(in);
		}
		return value;
	}
	
	/**
	 * Writes the specified byte array to the output stream. This method is 
	 * not thread safe.
	 * 
	 * @param array The byte array to compress
	 * @param buffer The byte array buffer used as input to the deflater. This
	 *               buffer must have enough space to hold the compressed data.
	 * @param compressor java.util.Deflater. 
	 * @param output The data output stream.
	 * @throws IOException Thrown if unable to write to the output stream.
	 */
	public static void writeByteArray(byte array[], byte buffer[], Deflater compressor, DataOutput output) throws IOException
	{
		// Compress the bytes
		 compressor.setInput(array);
		 compressor.finish();
		 int compressedDataLength = compressor.deflate(buffer);
		 DataSerializer.writeByteArray(buffer, compressedDataLength, output);
	}
	
	/**
	 * Reads byte array from the input stream. This method is not thread safe.
	 * 
	 * @param buffer The buffer to hold the decompressed data. This buffer
	 *               must be large enough to hold the decompressed data.
	 * @param decompressor java.util.Inflater
	 * @param input The data input stream.
	 * @return Returns the actual byte array (not compressed) read from the 
	 *         input stream.
	 * @throws IOException Thrown if unable to read from the input stream or
	 *                     unable to decompress the data.
	 */
	public static byte[] readByteArray(byte buffer[], Inflater decompressor, DataInput input) throws IOException
	{
		byte compressedBuffer[] = DataSerializer.readByteArray(input);	
		// Decompress the bytes
		decompressor.setInput(compressedBuffer, 0, compressedBuffer.length);
		byte retval[] = null;
		try {
			int resultLength = decompressor.inflate(buffer);
			retval = new byte[resultLength];
			System.arraycopy(compressedBuffer, 0, retval, 0, resultLength);
		} catch (DataFormatException e) {
			throw new IOException("Unable to decompress the byte array due to a data format error. " + e.getMessage());
		}
		return retval;
	}
	
	/**
	 * Reads UTF string from the input. This method is analogous to 
	 * DataInput.readUTF() except that it supports null string.
	 * @param input The data input stream.
	 * @return Returns null or non-null string value.
	 * @throws IOException Thrown if unable to read from the input stream.
	 */
	public static String readUTF(DataInput input) throws IOException
	{
		byte header = input.readByte();
		if (header == DSCODE.NULL_STRING) {
			return null;
		} 
		return input.readUTF();
	}
	
	/**
	 * Writes the specified sting value to the output stream. This method
	 * is analogous to DataOutput.writeUTF() except that it supports null
	 * string.
	 * @param value The string value to write to the output stream.
	 * @param output The data output stream.
	 * @throws IOException Thrown if unable to write to the output stream. 
	 */
	public static void writeUTF(String value, DataOutput output) throws IOException
	{
		if (value == null) {
			output.writeByte(DSCODE.NULL_STRING);
		} else {
			output.writeByte(DSCODE.STRING);
			output.writeUTF(value);
		}		
	}
	
	/**
	 * Reads boolean value;
	 */
	public static Boolean readBoolean(DataInput input) throws IOException
	{
		return input.readBoolean();
	}
	
	/**
	 * Writes the specified boolean value to the stream. If the value is null
	 * then it write false.
	 * @param value The value to write
	 * @param output
	 * @throws IOException
	 */
	public static void writeBoolean(Boolean value, DataOutput output) throws IOException
	{
		if (value == null) {
			output.writeBoolean(false);
		} else {
			output.writeBoolean(value);
		}
	}
	
	public static Byte readByte(DataInput input) throws IOException
	{
		return input.readByte();
	}
	
	public static void writeByte(Byte value, DataOutput output) throws IOException
	{
		if (value == null) {
			output.writeByte(0);
		} else {
			output.writeByte(value);
		}
	}
	
	public static Character readCharacter(DataInput input) throws IOException
	{
		return input.readChar();
	}
	
	public static void writeCharacter(Character value, DataOutput output) throws IOException
	{
		if (value == null) {
			output.writeChar(0);
		} else {
			output.writeChar(value);
		}
	}
	
	public static Double readDouble(DataInput input) throws IOException
	{
		return input.readDouble();
	}
	
	public static void writeDouble(Double value, DataOutput output) throws IOException
	{
		if (value == null) {
			output.writeDouble(0);
		} else {
			output.writeDouble(value);
		}
	}
	
	public static Float readFloat(DataInput input) throws IOException
	{
		return input.readFloat();
	}
	
	public static void writeFloat(Float value, DataOutput output) throws IOException
	{
		if (value == null) {
			output.writeFloat(0);
		} else {
			output.writeFloat(value);
		}
	}
	
	public static Integer readInteger(DataInput input) throws IOException
	{
		return input.readInt();
	}
	
	public static void writeInteger(Integer value, DataOutput output) throws IOException
	{
		if (value == null) {
			output.writeInt(0);
		} else {
			output.writeInt(value);
		}
	}
	
	public static Long readLong(DataInput input) throws IOException
	{
		return input.readLong();
	}
	
	public static void writeLong(Long value, DataOutput output) throws IOException
	{
		if (value == null) {
			output.writeLong(0);
		} else {
			output.writeLong(value);
		}
	}
	
	public static Short readShort(DataInput input) throws IOException
	{
		return input.readShort();
	}
	
	public static void writeShort(Short value, DataOutput output) throws IOException
	{
		if (value == null) {
			output.writeShort(0);
		} else {
			output.writeShort(value);
		}
	}
	
	public static MapLite readMapLite(DataInput input) throws ClassNotFoundException, IOException
	{
		byte header = input.readByte();
		if (header == DSCODE.NULL){
			return null;
		}
		MapLite fo = new MapLite();
		InternalDataSerializer.invokeFromData(fo, input);
		return fo;
	}
	
	public static void writeMapLite(MapLite value, DataOutput output) throws IOException
	{
		if (value == null) {
			output.writeByte(DSCODE.NULL);
		} else {
			output.writeByte(DSCODE.DS_NO_FIXED_ID);
			InternalDataSerializer.invokeToData(value, output);
		}
	}
}
