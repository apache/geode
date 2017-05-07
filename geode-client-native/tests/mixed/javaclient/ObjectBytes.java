/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 * 
 */
package javaclient;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Vector;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.Instantiator;

/**
 * @author xzhou
 *
 */
public class ObjectBytes implements DataSerializable {
    private byte[] content;
    private int id;

    static {
		Instantiator.register(new Instantiator(ObjectBytes.class, (byte) 54) {
			public DataSerializable newInstance() {
				return new ObjectBytes();
			}
		});
	}

    public byte[] getContent( ) {
        return this.content;
    }

    public int getId( ) {
        return this.id;
    }

    public void setContent( byte[] byte_array ) {
        this.content = byte_array;
    }

    public void setId( int id ) {
        this.id = id;
    }

	public void toData(DataOutput out) throws IOException {
		out.writeInt(id);
		out.writeInt(content.length);
 		out.write(content, 0, content.length);
	}

	public void fromData(DataInput in) throws IOException, ClassNotFoundException {
		this.id = in.readInt();
		int length = in.readInt();
		this.content= new byte[length];
		in.readFully(this.content);
	}
}
