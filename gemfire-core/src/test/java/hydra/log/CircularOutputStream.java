/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package hydra.log;

import java.io.*;

/**
 *  Implements a circular output stream with an upper limit on the number of bytes
 *  it contains.
 */
public class CircularOutputStream extends OutputStream {
  
  private static byte marker = '%';

  String name;
  int maxBytes;
  boolean rolling = false;
  RandomAccessFile raf;

  /**
   *  Constructs a new circular output stream.
   *  @param name the name of the output stream.
   *  @param maxBytes the maximum number of bytes in the output stream.
   *  @throws IOException if the stream cannot be created or written.
   */
  public CircularOutputStream( String name, int maxBytes )
  throws IOException {
    this.name = name;
    this.maxBytes = maxBytes;
    this.rolling = ( maxBytes > 0 );
    try {
      this.raf = new RandomAccessFile( name, "rw" );
    } catch( FileNotFoundException e ) {
      e.printStackTrace();
      throw new IOException( "Unable to create stream named " + name );
    }
    if ( this.rolling ) {
      // write the initial marker
      this.raf.write( marker );
    }
  }
  /**
   *  Implements {@link java.io.OutputStream#close}.
   */
  /*
  public void close() {
    this.raf.close();
  }
  */
  /**
   *  Implements {@link java.io.OutputStream#flush}.
   */
  /*
  public void flush() {
  }
  */
  /**
   *  Implements {@link java.io.OutputStream#write(byte[])}.
   */
  @Override
  public void write( byte[] b ) throws IOException {
    write( b, 0, b.length );
  }
  /**
   *  Implements {@link java.io.OutputStream#write(byte[],int,int)}.
   */
  @Override
  public void write( byte[] b, int off, int len ) throws IOException {
    if ( this.rolling ) {
      // back over marker character
      long fptr = this.raf.getFilePointer() - 1;
      this.raf.seek( fptr );
      // write bytes
      int space = (int)( this.maxBytes - fptr );
      if ( len <= space ) {
        this.raf.write( b, off, len );
      } else {
        this.raf.write( b, off, space );
        this.raf.seek(0);
        this.raf.write( b, off + space, len - space );
      }
      // wrap around if landed at the end
      if ( this.raf.getFilePointer() == this.maxBytes )
        this.raf.seek(0);
      // write marker character
      this.raf.write( marker );
    } else {
      this.raf.write( b, off, len );
    }
  }
  /**
   *  Implements {@link java.io.OutputStream#write(int)}.
   */
  @Override
  public void write( int b ) throws IOException {
    // back over marker character
    long fptr = this.raf.getFilePointer() - 1;
    this.raf.seek( fptr );
    // write byte
    this.raf.writeByte( b );
    // wrap around if landed at the end
    if ( this.raf.getFilePointer() == this.maxBytes )
      this.raf.seek(0);
    // write marker character
    this.raf.write( marker );
  }

  public static void main( String[] args ) throws IOException {
    CircularOutputStream t = new CircularOutputStream( "frip", 10 );
    PrintStream ps = new PrintStream( t, true ); // autoflush
    System.setOut( ps ); System.setErr( ps );

    System.out.println( "WHERE WILL THIS GO?" );
    String s = "AND WHAT ABOUT THIS?\n";
    t.write( s.getBytes() );
  }
}
