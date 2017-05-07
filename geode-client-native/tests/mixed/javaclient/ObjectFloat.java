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
public class ObjectFloat implements DataSerializable {
    private float content;
    private int id;

    static {
		Instantiator.register(new Instantiator(ObjectFloat.class, (byte) 48) {
			public DataSerializable newInstance() {
				return new ObjectFloat();
			}
		});
	}

    public float getContent( ) {
        return this.content;
    }

    public int getId( ) {
        return this.id;
    }

    public void setContent( float float_field ) {
        this.content = float_field;
    }

    public void setId( int id ) {
        this.id = id;
    }

	public void toData(DataOutput out) throws IOException {
		out.writeInt(id);
		out.writeFloat(content);
	}

	public void fromData(DataInput in) throws IOException, ClassNotFoundException {
		this.id = in.readInt();
		this.content = in.readFloat();
	}
}
