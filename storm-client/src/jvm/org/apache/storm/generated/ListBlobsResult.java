/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Autogenerated by Thrift Compiler (0.13.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.storm.generated;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.13.0)")
public class ListBlobsResult implements org.apache.storm.thrift.TBase<ListBlobsResult, ListBlobsResult._Fields>, java.io.Serializable, Cloneable, Comparable<ListBlobsResult> {
  private static final org.apache.storm.thrift.protocol.TStruct STRUCT_DESC = new org.apache.storm.thrift.protocol.TStruct("ListBlobsResult");

  private static final org.apache.storm.thrift.protocol.TField KEYS_FIELD_DESC = new org.apache.storm.thrift.protocol.TField("keys", org.apache.storm.thrift.protocol.TType.LIST, (short)1);
  private static final org.apache.storm.thrift.protocol.TField SESSION_FIELD_DESC = new org.apache.storm.thrift.protocol.TField("session", org.apache.storm.thrift.protocol.TType.STRING, (short)2);

  private static final org.apache.storm.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new ListBlobsResultStandardSchemeFactory();
  private static final org.apache.storm.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new ListBlobsResultTupleSchemeFactory();

  private @org.apache.storm.thrift.annotation.Nullable java.util.List<java.lang.String> keys; // required
  private @org.apache.storm.thrift.annotation.Nullable java.lang.String session; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.storm.thrift.TFieldIdEnum {
    KEYS((short)1, "keys"),
    SESSION((short)2, "session");

    private static final java.util.Map<java.lang.String, _Fields> byName = new java.util.HashMap<java.lang.String, _Fields>();

    static {
      for (_Fields field : java.util.EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    @org.apache.storm.thrift.annotation.Nullable
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // KEYS
          return KEYS;
        case 2: // SESSION
          return SESSION;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new java.lang.IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    @org.apache.storm.thrift.annotation.Nullable
    public static _Fields findByName(java.lang.String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final java.lang.String _fieldName;

    _Fields(short thriftId, java.lang.String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public java.lang.String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  public static final java.util.Map<_Fields, org.apache.storm.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.storm.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.storm.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.KEYS, new org.apache.storm.thrift.meta_data.FieldMetaData("keys", org.apache.storm.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.storm.thrift.meta_data.ListMetaData(org.apache.storm.thrift.protocol.TType.LIST, 
            new org.apache.storm.thrift.meta_data.FieldValueMetaData(org.apache.storm.thrift.protocol.TType.STRING))));
    tmpMap.put(_Fields.SESSION, new org.apache.storm.thrift.meta_data.FieldMetaData("session", org.apache.storm.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.storm.thrift.meta_data.FieldValueMetaData(org.apache.storm.thrift.protocol.TType.STRING)));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.storm.thrift.meta_data.FieldMetaData.addStructMetaDataMap(ListBlobsResult.class, metaDataMap);
  }

  public ListBlobsResult() {
  }

  public ListBlobsResult(
    java.util.List<java.lang.String> keys,
    java.lang.String session)
  {
    this();
    this.keys = keys;
    this.session = session;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public ListBlobsResult(ListBlobsResult other) {
    if (other.is_set_keys()) {
      java.util.List<java.lang.String> __this__keys = new java.util.ArrayList<java.lang.String>(other.keys);
      this.keys = __this__keys;
    }
    if (other.is_set_session()) {
      this.session = other.session;
    }
  }

  public ListBlobsResult deepCopy() {
    return new ListBlobsResult(this);
  }

  @Override
  public void clear() {
    this.keys = null;
    this.session = null;
  }

  public int get_keys_size() {
    return (this.keys == null) ? 0 : this.keys.size();
  }

  @org.apache.storm.thrift.annotation.Nullable
  public java.util.Iterator<java.lang.String> get_keys_iterator() {
    return (this.keys == null) ? null : this.keys.iterator();
  }

  public void add_to_keys(java.lang.String elem) {
    if (this.keys == null) {
      this.keys = new java.util.ArrayList<java.lang.String>();
    }
    this.keys.add(elem);
  }

  @org.apache.storm.thrift.annotation.Nullable
  public java.util.List<java.lang.String> get_keys() {
    return this.keys;
  }

  public void set_keys(@org.apache.storm.thrift.annotation.Nullable java.util.List<java.lang.String> keys) {
    this.keys = keys;
  }

  public void unset_keys() {
    this.keys = null;
  }

  /** Returns true if field keys is set (has been assigned a value) and false otherwise */
  public boolean is_set_keys() {
    return this.keys != null;
  }

  public void set_keys_isSet(boolean value) {
    if (!value) {
      this.keys = null;
    }
  }

  @org.apache.storm.thrift.annotation.Nullable
  public java.lang.String get_session() {
    return this.session;
  }

  public void set_session(@org.apache.storm.thrift.annotation.Nullable java.lang.String session) {
    this.session = session;
  }

  public void unset_session() {
    this.session = null;
  }

  /** Returns true if field session is set (has been assigned a value) and false otherwise */
  public boolean is_set_session() {
    return this.session != null;
  }

  public void set_session_isSet(boolean value) {
    if (!value) {
      this.session = null;
    }
  }

  public void setFieldValue(_Fields field, @org.apache.storm.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case KEYS:
      if (value == null) {
        unset_keys();
      } else {
        set_keys((java.util.List<java.lang.String>)value);
      }
      break;

    case SESSION:
      if (value == null) {
        unset_session();
      } else {
        set_session((java.lang.String)value);
      }
      break;

    }
  }

  @org.apache.storm.thrift.annotation.Nullable
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case KEYS:
      return get_keys();

    case SESSION:
      return get_session();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case KEYS:
      return is_set_keys();
    case SESSION:
      return is_set_session();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that == null)
      return false;
    if (that instanceof ListBlobsResult)
      return this.equals((ListBlobsResult)that);
    return false;
  }

  public boolean equals(ListBlobsResult that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_keys = true && this.is_set_keys();
    boolean that_present_keys = true && that.is_set_keys();
    if (this_present_keys || that_present_keys) {
      if (!(this_present_keys && that_present_keys))
        return false;
      if (!this.keys.equals(that.keys))
        return false;
    }

    boolean this_present_session = true && this.is_set_session();
    boolean that_present_session = true && that.is_set_session();
    if (this_present_session || that_present_session) {
      if (!(this_present_session && that_present_session))
        return false;
      if (!this.session.equals(that.session))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + ((is_set_keys()) ? 131071 : 524287);
    if (is_set_keys())
      hashCode = hashCode * 8191 + keys.hashCode();

    hashCode = hashCode * 8191 + ((is_set_session()) ? 131071 : 524287);
    if (is_set_session())
      hashCode = hashCode * 8191 + session.hashCode();

    return hashCode;
  }

  @Override
  public int compareTo(ListBlobsResult other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.valueOf(is_set_keys()).compareTo(other.is_set_keys());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_keys()) {
      lastComparison = org.apache.storm.thrift.TBaseHelper.compareTo(this.keys, other.keys);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(is_set_session()).compareTo(other.is_set_session());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_session()) {
      lastComparison = org.apache.storm.thrift.TBaseHelper.compareTo(this.session, other.session);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  @org.apache.storm.thrift.annotation.Nullable
  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.storm.thrift.protocol.TProtocol iprot) throws org.apache.storm.thrift.TException {
    scheme(iprot).read(iprot, this);
  }

  public void write(org.apache.storm.thrift.protocol.TProtocol oprot) throws org.apache.storm.thrift.TException {
    scheme(oprot).write(oprot, this);
  }

  @Override
  public java.lang.String toString() {
    java.lang.StringBuilder sb = new java.lang.StringBuilder("ListBlobsResult(");
    boolean first = true;

    sb.append("keys:");
    if (this.keys == null) {
      sb.append("null");
    } else {
      sb.append(this.keys);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("session:");
    if (this.session == null) {
      sb.append("null");
    } else {
      sb.append(this.session);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.storm.thrift.TException {
    // check for required fields
    if (!is_set_keys()) {
      throw new org.apache.storm.thrift.protocol.TProtocolException("Required field 'keys' is unset! Struct:" + toString());
    }

    if (!is_set_session()) {
      throw new org.apache.storm.thrift.protocol.TProtocolException("Required field 'session' is unset! Struct:" + toString());
    }

    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.storm.thrift.protocol.TCompactProtocol(new org.apache.storm.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.storm.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, java.lang.ClassNotFoundException {
    try {
      read(new org.apache.storm.thrift.protocol.TCompactProtocol(new org.apache.storm.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.storm.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class ListBlobsResultStandardSchemeFactory implements org.apache.storm.thrift.scheme.SchemeFactory {
    public ListBlobsResultStandardScheme getScheme() {
      return new ListBlobsResultStandardScheme();
    }
  }

  private static class ListBlobsResultStandardScheme extends org.apache.storm.thrift.scheme.StandardScheme<ListBlobsResult> {

    public void read(org.apache.storm.thrift.protocol.TProtocol iprot, ListBlobsResult struct) throws org.apache.storm.thrift.TException {
      org.apache.storm.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.storm.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // KEYS
            if (schemeField.type == org.apache.storm.thrift.protocol.TType.LIST) {
              {
                org.apache.storm.thrift.protocol.TList _list664 = iprot.readListBegin();
                struct.keys = new java.util.ArrayList<java.lang.String>(_list664.size);
                @org.apache.storm.thrift.annotation.Nullable java.lang.String _elem665;
                for (int _i666 = 0; _i666 < _list664.size; ++_i666)
                {
                  _elem665 = iprot.readString();
                  struct.keys.add(_elem665);
                }
                iprot.readListEnd();
              }
              struct.set_keys_isSet(true);
            } else { 
              org.apache.storm.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // SESSION
            if (schemeField.type == org.apache.storm.thrift.protocol.TType.STRING) {
              struct.session = iprot.readString();
              struct.set_session_isSet(true);
            } else { 
              org.apache.storm.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.storm.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();
      struct.validate();
    }

    public void write(org.apache.storm.thrift.protocol.TProtocol oprot, ListBlobsResult struct) throws org.apache.storm.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.keys != null) {
        oprot.writeFieldBegin(KEYS_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.storm.thrift.protocol.TList(org.apache.storm.thrift.protocol.TType.STRING, struct.keys.size()));
          for (java.lang.String _iter667 : struct.keys)
          {
            oprot.writeString(_iter667);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.session != null) {
        oprot.writeFieldBegin(SESSION_FIELD_DESC);
        oprot.writeString(struct.session);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class ListBlobsResultTupleSchemeFactory implements org.apache.storm.thrift.scheme.SchemeFactory {
    public ListBlobsResultTupleScheme getScheme() {
      return new ListBlobsResultTupleScheme();
    }
  }

  private static class ListBlobsResultTupleScheme extends org.apache.storm.thrift.scheme.TupleScheme<ListBlobsResult> {

    @Override
    public void write(org.apache.storm.thrift.protocol.TProtocol prot, ListBlobsResult struct) throws org.apache.storm.thrift.TException {
      org.apache.storm.thrift.protocol.TTupleProtocol oprot = (org.apache.storm.thrift.protocol.TTupleProtocol) prot;
      {
        oprot.writeI32(struct.keys.size());
        for (java.lang.String _iter668 : struct.keys)
        {
          oprot.writeString(_iter668);
        }
      }
      oprot.writeString(struct.session);
    }

    @Override
    public void read(org.apache.storm.thrift.protocol.TProtocol prot, ListBlobsResult struct) throws org.apache.storm.thrift.TException {
      org.apache.storm.thrift.protocol.TTupleProtocol iprot = (org.apache.storm.thrift.protocol.TTupleProtocol) prot;
      {
        org.apache.storm.thrift.protocol.TList _list669 = new org.apache.storm.thrift.protocol.TList(org.apache.storm.thrift.protocol.TType.STRING, iprot.readI32());
        struct.keys = new java.util.ArrayList<java.lang.String>(_list669.size);
        @org.apache.storm.thrift.annotation.Nullable java.lang.String _elem670;
        for (int _i671 = 0; _i671 < _list669.size; ++_i671)
        {
          _elem670 = iprot.readString();
          struct.keys.add(_elem670);
        }
      }
      struct.set_keys_isSet(true);
      struct.session = iprot.readString();
      struct.set_session_isSet(true);
    }
  }

  private static <S extends org.apache.storm.thrift.scheme.IScheme> S scheme(org.apache.storm.thrift.protocol.TProtocol proto) {
    return (org.apache.storm.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
}

