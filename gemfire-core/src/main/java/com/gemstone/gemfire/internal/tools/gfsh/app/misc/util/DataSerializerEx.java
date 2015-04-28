package com.gemstone.gemfire.internal.tools.gfsh.app.misc.util;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.DSCODE;

public abstract class DataSerializerEx extends DataSerializer
{
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
}
 