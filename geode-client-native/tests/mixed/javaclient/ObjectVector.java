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
public class ObjectVector implements DataSerializable {
    private Vector content;
    private int id;

    static {
		Instantiator.register(new Instantiator(ObjectVector.class, (byte) 53) {
			public DataSerializable newInstance() {
				return new ObjectVector();
			}
		});
	}

    public Vector getContent( ) {
        return this.content;
    }

    public int getId( ) {
        return this.id;
    }

    public void setString_Vector( Vector string_vector ) {
        this.content = string_vector;
    }

    public void setId( int id ) {
        this.id = id;
    }

	public void toData(DataOutput out) throws IOException {
		out.writeInt(id);
		out.writeInt(content.size());
		for (int i=0; i<content.size(); i++) {
			out.writeUTF((String)content.elementAt(i));
		}
	}

	public void fromData(DataInput in) throws IOException, ClassNotFoundException {
		this.id = in.readInt();
		int size = in.readInt();
		for (int i=0; i<size; i++) {
			String s = in.readUTF();
			content.add(i, s);
		}
	}
}
