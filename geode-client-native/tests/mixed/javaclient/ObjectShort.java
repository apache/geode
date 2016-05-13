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
public class ObjectShort implements DataSerializable {
    private short content;
    private int id;

    static {
		Instantiator.register(new Instantiator(ObjectShort.class, (byte) 51) {
			public DataSerializable newInstance() {
				return new ObjectShort();
			}
		});
	}

    public short getContent( ) {
        return this.content;
    }

    public int getId( ) {
        return this.id;
    }

    public void setContent( short short_field ) {
        this.content = short_field;
    }

    public void setId( int id ) {
        this.id = id;
    }

	public void toData(DataOutput out) throws IOException {
		out.writeInt(id);
		out.writeShort(content);
	}

	public void fromData(DataInput in) throws IOException, ClassNotFoundException {
		this.id = in.readInt();
		this.content = in.readShort();
	}
}
