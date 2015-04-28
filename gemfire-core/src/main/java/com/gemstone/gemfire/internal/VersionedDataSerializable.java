package com.gemstone.gemfire.internal;

import com.gemstone.gemfire.DataSerializable;

/**
 * An extension of DataSerializable that can support multiple serialized
 * forms for backward compatibility.  DataSerializer.writeObject/readObject
 * will invoke getDSFIDVersions() and for each version returned it will
 * use the corresponding toDataPreXXX/fromDataPreXXX methods when serializing
 * for a particular version of the product.
 * 
 * @author bruces
 *
 */
public interface VersionedDataSerializable extends DataSerializable, SerializationVersions {


}
