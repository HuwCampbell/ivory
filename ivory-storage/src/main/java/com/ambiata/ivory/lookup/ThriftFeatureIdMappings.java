/**
 * Autogenerated by Thrift Compiler (0.9.1)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package com.ambiata.ivory.lookup;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.server.AbstractNonblockingServer.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftFeatureIdMappings implements org.apache.thrift.TBase<ThriftFeatureIdMappings, ThriftFeatureIdMappings._Fields>, java.io.Serializable, Cloneable, Comparable<ThriftFeatureIdMappings> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("ThriftFeatureIdMappings");

  private static final org.apache.thrift.protocol.TField FEATURES_FIELD_DESC = new org.apache.thrift.protocol.TField("features", org.apache.thrift.protocol.TType.LIST, (short)1);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new ThriftFeatureIdMappingsStandardSchemeFactory());
    schemes.put(TupleScheme.class, new ThriftFeatureIdMappingsTupleSchemeFactory());
  }

  public List<String> features; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    FEATURES((short)1, "features");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // FEATURES
          return FEATURES;
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
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.FEATURES, new org.apache.thrift.meta_data.FieldMetaData("features", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(ThriftFeatureIdMappings.class, metaDataMap);
  }

  public ThriftFeatureIdMappings() {
  }

  public ThriftFeatureIdMappings(
    List<String> features)
  {
    this();
    this.features = features;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public ThriftFeatureIdMappings(ThriftFeatureIdMappings other) {
    if (other.isSetFeatures()) {
      List<String> __this__features = new ArrayList<String>(other.features);
      this.features = __this__features;
    }
  }

  public ThriftFeatureIdMappings deepCopy() {
    return new ThriftFeatureIdMappings(this);
  }

  @Override
  public void clear() {
    this.features = null;
  }

  public int getFeaturesSize() {
    return (this.features == null) ? 0 : this.features.size();
  }

  public java.util.Iterator<String> getFeaturesIterator() {
    return (this.features == null) ? null : this.features.iterator();
  }

  public void addToFeatures(String elem) {
    if (this.features == null) {
      this.features = new ArrayList<String>();
    }
    this.features.add(elem);
  }

  public List<String> getFeatures() {
    return this.features;
  }

  public ThriftFeatureIdMappings setFeatures(List<String> features) {
    this.features = features;
    return this;
  }

  public void unsetFeatures() {
    this.features = null;
  }

  /** Returns true if field features is set (has been assigned a value) and false otherwise */
  public boolean isSetFeatures() {
    return this.features != null;
  }

  public void setFeaturesIsSet(boolean value) {
    if (!value) {
      this.features = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case FEATURES:
      if (value == null) {
        unsetFeatures();
      } else {
        setFeatures((List<String>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case FEATURES:
      return getFeatures();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case FEATURES:
      return isSetFeatures();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof ThriftFeatureIdMappings)
      return this.equals((ThriftFeatureIdMappings)that);
    return false;
  }

  public boolean equals(ThriftFeatureIdMappings that) {
    if (that == null)
      return false;

    boolean this_present_features = true && this.isSetFeatures();
    boolean that_present_features = true && that.isSetFeatures();
    if (this_present_features || that_present_features) {
      if (!(this_present_features && that_present_features))
        return false;
      if (!this.features.equals(that.features))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public int compareTo(ThriftFeatureIdMappings other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetFeatures()).compareTo(other.isSetFeatures());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetFeatures()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.features, other.features);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("ThriftFeatureIdMappings(");
    boolean first = true;

    sb.append("features:");
    if (this.features == null) {
      sb.append("null");
    } else {
      sb.append(this.features);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class ThriftFeatureIdMappingsStandardSchemeFactory implements SchemeFactory {
    public ThriftFeatureIdMappingsStandardScheme getScheme() {
      return new ThriftFeatureIdMappingsStandardScheme();
    }
  }

  private static class ThriftFeatureIdMappingsStandardScheme extends StandardScheme<ThriftFeatureIdMappings> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, ThriftFeatureIdMappings struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // FEATURES
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list114 = iprot.readListBegin();
                struct.features = new ArrayList<String>(_list114.size);
                for (int _i115 = 0; _i115 < _list114.size; ++_i115)
                {
                  String _elem116;
                  _elem116 = iprot.readString();
                  struct.features.add(_elem116);
                }
                iprot.readListEnd();
              }
              struct.setFeaturesIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, ThriftFeatureIdMappings struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.features != null) {
        oprot.writeFieldBegin(FEATURES_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, struct.features.size()));
          for (String _iter117 : struct.features)
          {
            oprot.writeString(_iter117);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class ThriftFeatureIdMappingsTupleSchemeFactory implements SchemeFactory {
    public ThriftFeatureIdMappingsTupleScheme getScheme() {
      return new ThriftFeatureIdMappingsTupleScheme();
    }
  }

  private static class ThriftFeatureIdMappingsTupleScheme extends TupleScheme<ThriftFeatureIdMappings> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, ThriftFeatureIdMappings struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetFeatures()) {
        optionals.set(0);
      }
      oprot.writeBitSet(optionals, 1);
      if (struct.isSetFeatures()) {
        {
          oprot.writeI32(struct.features.size());
          for (String _iter118 : struct.features)
          {
            oprot.writeString(_iter118);
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, ThriftFeatureIdMappings struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(1);
      if (incoming.get(0)) {
        {
          org.apache.thrift.protocol.TList _list119 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, iprot.readI32());
          struct.features = new ArrayList<String>(_list119.size);
          for (int _i120 = 0; _i120 < _list119.size; ++_i120)
          {
            String _elem121;
            _elem121 = iprot.readString();
            struct.features.add(_elem121);
          }
        }
        struct.setFeaturesIsSet(true);
      }
    }
  }

}
