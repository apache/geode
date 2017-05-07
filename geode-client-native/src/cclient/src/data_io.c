#include "data_io.h"
#include <stdio.h>
/**
 * Write an unsigned byte to the <code>DataOutput</code>.
 *
 * @param value the unsigned byte to be written
 */
inline void writeUnsigned(Buffer* buf, uint8_t value)
{
    ensureCapacity(buf,1);
    writeNoCheck(buf, value);
}

/**
 * Write a signed byte to the <code>DataOutput</code>.
 *
 * @param value the signed byte to be written
 */
inline void writeByte(Buffer* buf, int8_t value)
{
    writeUnsigned(buf, (uint8_t)value);
}

/**
 * Read a signed byte from the <code>DataInput</code>.
 *
 * @param value output parameter to hold the signed byte read from stream
 */
inline void readByte(Buffer* buf, int8_t* value )
{
    //checkBufferSize(1);
    *value = *(buf->m_buf++);
}

/**
 *    * Read a 16-bit signed integer from the <code>DataInput</code>.
 *       *
 *          * @param value output parameter to hold the 16-bit signed integer
 *             *   read from stream
 *                */
inline void readShort(Buffer* buf, int16_t* value )
{
//    checkBufferSize(2);
    readUnsignedShort(buf, (uint16_t*)value );
}


/**
 *    * Read a 16-bit unsigned integer from the <code>DataInput</code>.
 *       *
 *          * @param value output parameter to hold the 16-bit unsigned integer
 *             *   read from stream
 *                */
inline void readUnsignedShort(Buffer* buf, uint16_t* value )
{
//    checkBufferSize(2);
    uint16_t tmp = *(buf->m_buf++);
    tmp = (tmp << 8) | *(buf->m_buf++);
    *value = tmp;
}


/**
 * Read a 32-bit signed integer from the <code>DataInput</code>.
 *
 * @param value output parameter to hold the 32-bit signed integer
 *   read from stream
 */
inline void readInt(Buffer* buf, int32_t* value )
{
    //checkBufferSize(4);
    readUnsignedInt(buf, (uint32_t*)value );
}


/**
 * Read a 32-bit unsigned integer from the <code>DataInput</code>.
 *
 * @param value output parameter to hold the 32-bit unsigned integer
 *   read from stream
 */
inline void readUnsignedInt(Buffer* buf, uint32_t* value )
{
    //checkBufferSize(4);
    uint32_t tmp = *(buf->m_buf++);
    tmp = (tmp << 8) | *(buf->m_buf++);
    tmp = (tmp << 8) | *(buf->m_buf++);
    tmp = (tmp << 8) | *(buf->m_buf++);
    *value = tmp;
}




/**
 * Write an array of unsigned bytes to the <code>DataOutput</code>.
 *
 * @param value the array of unsigned bytes to be written
 * @param len the number of bytes from the start of array to be written
 */
inline void writeUnsignedBytes(Buffer* buf,  const uint8_t* bytes, int32_t len )
{
    if (len >= 0) {
        ensureCapacity(buf, len + 5 );
        writeArrayLen(buf,  bytes==NULL ? 0 : len ); // length of bytes...
        if ( len > 0 && bytes != NULL) {
            memcpy( buf->m_buf, bytes, len );
            buf->m_buf += len;
        }
    } else {
        writeByte(buf, (int8_t) -1 );
    }
}

/**
 * Write an array of signed bytes to the <code>DataOutput</code>.
 *
 * @param value the array of signed bytes to be written
 * @param len the number of bytes from the start of array to be written
 */
inline void writeBytes(Buffer* buf,  const int8_t* bytes, int32_t len )
{
   // printf("bytes length: %d\n", len);
    writeUnsignedBytes(buf, (const uint8_t*)bytes, len );
}
/**
 * Write a 32-bit unsigned integer value to the <code>DataOutput</code>.
 *
 * @param value the 32-bit unsigned integer value to be written
 */
inline void writeUnsignedInt( Buffer* buf,uint32_t value )
{
    ensureCapacity(buf, 4 );
    *(buf->m_buf++) = (uint8_t)(value >> 24);
    *(buf->m_buf++) = (uint8_t)(value >> 16);
    *(buf->m_buf++) = (uint8_t)(value >> 8);
    *(buf->m_buf++) = (uint8_t)value;
}

/**
 * Write a 32-bit signed integer value to the <code>DataOutput</code>.
 *
 * @param value the 32-bit signed integer value to be written
 */
inline void writeInt(Buffer* buf,  int32_t value )
{
    writeUnsignedInt(buf, (uint32_t)value );
}

/**
 * Write a 64-bit unsigned integer value to the <code>DataOutput</code>.
 *
 * @param value the 64-bit unsigned integer value to be written
 */
inline void writeUnsignedLong( Buffer* buf,uint64_t value )
{
    ensureCapacity(buf, 8 );
    *(buf->m_buf++) = (uint8_t)(value >> 56);
    *(buf->m_buf++) = (uint8_t)(value >> 48);
    *(buf->m_buf++) = (uint8_t)(value >> 40);
    *(buf->m_buf++) = (uint8_t)(value >> 32);
    *(buf->m_buf++) = (uint8_t)(value >> 24);
    *(buf->m_buf++) = (uint8_t)(value >> 16);
    *(buf->m_buf++) = (uint8_t)(value >> 8);
    *(buf->m_buf++) = (uint8_t)value;
}

/**
 * Write a 64-bit signed integer value to the <code>DataOutput</code>.
 *
 * @param value the 64-bit signed integer value to be written
 */
inline void writeLong(Buffer* buf,  int64_t value )
{
    writeUnsignedLong(buf, (uint64_t)value );
}

/**
 * Write a 16-bit unsigned integer value to the <code>DataOutput</code>.
 *
 * @param value the 16-bit unsigned integer value to be written
 */
inline void writeUnsignedShort( Buffer* buf,uint16_t value )
{
    ensureCapacity(buf, 2 );
    *(buf->m_buf++) = (uint8_t)(value >> 8);
    *(buf->m_buf++) = (uint8_t)value;
}

/**
 * Write a 16-bit signed integer value to the <code>DataOutput</code>.
 *
 * @param value the 16-bit signed integer value to be written
 */
inline void writeShort(Buffer* buf,  int16_t value )
{
    writeUnsignedShort(buf, (uint16_t)value );
}

/**
 * Write a 32-bit signed integer array length value to the
 * <code>DataOutput</code> in a manner compatible with java server's
 * <code>DataSerializer.writeArrayLength</code>.
 *
 * @param value the 32-bit signed integer array length to be written
 */
inline void writeArrayLen(Buffer* buf,  int32_t len )
{
    if (len == -1) {
        writeByte(buf, (int8_t) -1);
    } else if (len <= 252) { // 252 is java's ((byte)-4 && 0xFF)
        writeUnsigned(buf, (uint8_t)len);
    } else if (len <= 0xFFFF) {
        writeByte(buf, (int8_t) -2);
        writeUnsignedShort(buf, (uint16_t)len);
    } else {
        writeByte(buf, (int8_t) -3);
        writeInt(buf, len);
    }
}

/**
 * Advance the buffer cursor by the given offset.
 *
 * @param offset the offset by which to advance the cursor
 */
void advanceCursor(Buffer* buf, uint32_t offset)
{
    buf->m_buf += offset;
}

/**
 * Rewind the buffer cursor by the given offset.
 *
 * @param offset the offset by which to rewind the cursor
 */
void rewindCursor(Buffer* buf, uint32_t offset)
{
    buf->m_buf -= offset;
}

/**
 * Get the length of current data in the internal buffer of
 * <code>DataOutput</code>.
 */
inline uint32_t getBufferLength(Buffer* buf) {
    return (uint32_t)( buf->m_buf - buf->m_bytes );
}

inline uint8_t* getBuffer(Buffer* buf)
{
    return buf->m_bytes;
}

inline uint8_t* getCursor(Buffer* buf) {
    return buf->m_buf;
}

// make sure there is room left for the requested size item.
inline void ensureCapacity(Buffer* buf,  uint32_t size )
{
    uint32_t offset = (uint32_t)( buf->m_buf - buf->m_bytes );
    if ( (buf->m_size - offset) < size ) {
        uint32_t newSize = buf->m_size * 2 + (8192 * (size / 8192));
        buf->m_size = newSize;
        GF_RESIZE( buf->m_bytes, uint8_t, buf->m_size );
        buf->m_buf = buf->m_bytes + offset;
    }
}

inline void writeNoCheck(Buffer* buf, uint8_t value)
{
    //uint32_t offset = (uint32_t)( buf->m_buf - buf->m_bytes );
    //printf("%d:%d\n",offset, buf->m_size);
    *(buf->m_buf++) = value;
}


inline void initBuf(Buffer* buf)
{
    GF_ALLOC( buf->m_bytes, uint8_t, 8192 );
    buf->m_buf = buf->m_bytes;
    buf->m_size = 8192;
}

inline void clearBuf(Buffer* buf)
{
    GF_FREE(buf->m_bytes);
    buf->m_bytes = NULL;
    buf->m_buf = NULL;
    buf->m_size = 0;
}

inline void writeASCII(Buffer* buf, const char* value)
{
    if ( value != NULL ) {
      uint32_t length = (uint32_t)strlen( value );
      uint16_t len = (uint16_t)( length > 0xFFFF ? 0xFFFF : length );
      writeUnsignedShort(buf, len);
      writeBytesOnly(buf, (int8_t*)value, len ); // K64
    } else {
      writeShort(buf, (uint16_t)0);
    }
}

inline void writeBytesOnly(Buffer* buf,  const int8_t* bytes, int32_t len)
{
    ensureCapacity(buf, len);
    memcpy(buf->m_buf, bytes, len);
    buf->m_buf += len;
}

