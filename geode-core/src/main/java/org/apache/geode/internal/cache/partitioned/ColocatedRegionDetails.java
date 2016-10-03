/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geode.internal.cache.partitioned;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.DistributedMember;

/**
 * Composite data type used to record colocation relationships
 */
public class ColocatedRegionDetails implements DataSerializable {
  private String host;
  private String member;
  private String parent;
  private String child;

  public ColocatedRegionDetails(final String host, final String member, final String parent, final String child) {
    this.host = host;
    this.member = member;
    this.parent = parent;
    this.child = child;
  }

  //Used for deserialization only
  public ColocatedRegionDetails() {
  }

  /**
   * Returns the canonical name of the host machine
   * 
   * @return parent
   */
  public String getHost() {
    return host;
  }

  /**
   * Returns the name of the {@link DistributedMember}
   * 
   * @return parent
   */
  public String getMember() {
    return member;
  }

  /**
   * Returns the name of the parent region of a colocated pair
   * 
   * @return parent
   */
  public String getParent() {
    return parent;
  }

  /**
   * Returns the name of the child region of a colocated pair
   * 
   * @return child
   */
  public String getChild() {
    return child;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    boolean hasHost = in.readBoolean();
    if(hasHost) {
      host = DataSerializer.readString(in);
    }
    boolean hasMember = in.readBoolean();
    if (hasMember) {
      member = DataSerializer.readString(in);
    }
    boolean hasParent = in.readBoolean();
    if (hasParent) {
      parent = DataSerializer.readString(in);
    }
    boolean hasChild = in.readBoolean();
    if (hasChild) {
      child = DataSerializer.readString(in);
    }
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    out.writeBoolean(host != null);
    if(host != null) {
      DataSerializer.writeString(host, out);
    }
    out.writeBoolean(member != null);
    if (member != null) {
      DataSerializer.writeString(member, out);
    }
    out.writeBoolean(parent != null);
    if (parent != null) {
      DataSerializer.writeString(parent, out);
    }
    out.writeBoolean(child != null);
    if (child != null) {
      DataSerializer.writeString(child, out);
    }
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append("[");
    if(host != null) {
      result.append("host:" + host.toString());
    } else {
      result.append("");
    }
    if(member != null) {
      result.append(", member:" + member.toString());
    } else {
      result.append(",");
    }
    if(parent != null) {
      result.append(", parent:" + parent.toString());
    } else {
      result.append(",");
    }
    if(child != null) {
      result.append(", child:" + child.toString());
    } else {
      result.append(",");
    }
    result.append("]");

    return result.toString();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((host == null) ? 0 : host.hashCode());
    result = prime * result + ((member == null) ? 0 : member.hashCode());
    result = prime * result + ((parent == null) ? 0 : parent.hashCode());
    result = prime * result + ((child == null) ? 0 : child.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    ColocatedRegionDetails other = (ColocatedRegionDetails) obj;
    if (host == null) {
      if (other.host != null) {
        return false;
      }
    } else if (!host.equals(other.host)) {
      return false;
    }
    if (member == null) {
      if (other.member != null) {
        return false;
      }
    } else if (!member.equals(other.member)) {
      return false;
    }
    if (parent == null) {
      if (other.parent != null) {
        return false;
      }
    } else if (!parent.equals(other.parent)) {
      return false;
    }
    if (child == null) {
      if (other.child != null) {
        return false;
      }
    } else if (!child.equals(other.child)) {
      return false;
    }
    return true;
  }

}
