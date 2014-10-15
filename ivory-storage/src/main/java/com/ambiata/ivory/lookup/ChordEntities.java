/**
 * This file has been hand-tweaked to use {@code int[]} instead of {@code List<Integer>} for maximum performance.
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

public class ChordEntities implements org.apache.thrift.TBase<ChordEntities, ChordEntities._Fields>, java.io.Serializable, Cloneable, Comparable<ChordEntities> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("ChordEntities");

  private static final org.apache.thrift.protocol.TField ENTITIES_FIELD_DESC = new org.apache.thrift.protocol.TField("entities", org.apache.thrift.protocol.TType.MAP, (short)1);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new ChordEntitiesStandardSchemeFactory());
    schemes.put(TupleScheme.class, new ChordEntitiesTupleSchemeFactory());
  }

  public Map<String, int[]> entities; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    ENTITIES((short)1, "entities");

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
        case 1: // ENTITIES
          return ENTITIES;
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
    tmpMap.put(_Fields.ENTITIES, new org.apache.thrift.meta_data.FieldMetaData("entities", org.apache.thrift.TFieldRequirementType.DEFAULT,
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP,
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING),
            new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST,
                new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(ChordEntities.class, metaDataMap);
  }

  public ChordEntities() {
  }

  public ChordEntities(
    Map<String,int[]> entities)
  {
    this();
    this.entities = entities;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public ChordEntities(ChordEntities other) {
    if (other.isSetEntities()) {
      Map<String, int[]> __this__entities = new HashMap<String, int[]>(other.entities.size());
      for (Map.Entry<String, int[]> other_element : other.entities.entrySet()) {

        String other_element_key = other_element.getKey();
        int[] other_element_value = other_element.getValue();

        int[] __this__entities_copy_value = new int[other_element_value.length];
        System.arraycopy(other_element_value, 0,  __this__entities_copy_value, 0, __this__entities_copy_value.length);

        __this__entities.put(other_element_key, __this__entities_copy_value);
      }
      this.entities = __this__entities;
    }
  }

  public ChordEntities deepCopy() {
    return new ChordEntities(this);
  }

  @Override
  public void clear() {
    this.entities = null;
  }

  public int getEntitiesSize() {
    return (this.entities == null) ? 0 : this.entities.size();
  }

  public void putToEntities(String key, int[] val) {
    if (this.entities == null) {
      this.entities = new HashMap<String, int[]>();
    }
    this.entities.put(key, val);
  }

  public Map<String, int[]> getEntities() {
    return this.entities;
  }

  public ChordEntities setEntities(Map<String, int[]> entities) {
    this.entities = entities;
    return this;
  }

  public void unsetEntities() {
    this.entities = null;
  }

  /** Returns true if field entities is set (has been assigned a value) and false otherwise */
  public boolean isSetEntities() {
    return this.entities != null;
  }

  public void setEntitiesIsSet(boolean value) {
    if (!value) {
      this.entities = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case ENTITIES:
      if (value == null) {
        unsetEntities();
      } else {
        setEntities((Map<String, int[]>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case ENTITIES:
      return getEntities();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case ENTITIES:
      return isSetEntities();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof ChordEntities)
      return this.equals((ChordEntities)that);
    return false;
  }

  public boolean equals(ChordEntities that) {
    if (that == null)
      return false;

    boolean this_present_entities = true && this.isSetEntities();
    boolean that_present_entities = true && that.isSetEntities();
    if (this_present_entities || that_present_entities) {
      if (!(this_present_entities && that_present_entities))
        return false;
      if (!this.entities.equals(that.entities))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public int compareTo(ChordEntities other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetEntities()).compareTo(other.isSetEntities());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetEntities()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.entities, other.entities);
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
    StringBuilder sb = new StringBuilder("ChordEntities(");
    boolean first = true;

    sb.append("entities:");
    if (this.entities == null) {
      sb.append("null");
    } else {
      sb.append(this.entities);
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

  private static class ChordEntitiesStandardSchemeFactory implements SchemeFactory {
    public ChordEntitiesStandardScheme getScheme() {
      return new ChordEntitiesStandardScheme();
    }
  }

  private static class ChordEntitiesStandardScheme extends StandardScheme<ChordEntities> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, ChordEntities struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) {
          break;
        }
        switch (schemeField.id) {
          case 1: // ENTITIES
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map88 = iprot.readMapBegin();
                struct.entities = new HashMap<String, int[]>(2*_map88.size);
                for (int _i89 = 0; _i89 < _map88.size; ++_i89)
                {
                  String _key90;
                  int[] _val91;
                  _key90 = iprot.readString();
                  {
                    org.apache.thrift.protocol.TList _list92 = iprot.readListBegin();
                    _val91 = new int[_list92.size];
                    for (int _i93 = 0; _i93 < _list92.size; ++_i93)
                    {
                      int _elem94;
                      _elem94 = iprot.readI32();
                      _val91[_i93] = _elem94;
                    }
                    iprot.readListEnd();
                  }
                  struct.entities.put(_key90, _val91);
                }
                iprot.readMapEnd();
              }
              struct.setEntitiesIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, ChordEntities struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.entities != null) {
        oprot.writeFieldBegin(ENTITIES_FIELD_DESC);
        {
          oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.LIST, struct.entities.size()));
          for (Map.Entry<String, int[]> _iter95 : struct.entities.entrySet())
          {
            oprot.writeString(_iter95.getKey());
            {
              oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.I32, _iter95.getValue().length));
              for (int _iter96 : _iter95.getValue())
              {
                oprot.writeI32(_iter96);
              }
              oprot.writeListEnd();
            }
          }
          oprot.writeMapEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class ChordEntitiesTupleSchemeFactory implements SchemeFactory {
    public ChordEntitiesTupleScheme getScheme() {
      return new ChordEntitiesTupleScheme();
    }
  }

  private static class ChordEntitiesTupleScheme extends TupleScheme<ChordEntities> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, ChordEntities struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetEntities()) {
        optionals.set(0);
      }
      oprot.writeBitSet(optionals, 1);
      if (struct.isSetEntities()) {
        {
          oprot.writeI32(struct.entities.size());
          for (Map.Entry<String, int[]> _iter97 : struct.entities.entrySet())
          {
            oprot.writeString(_iter97.getKey());
            {
              oprot.writeI32(_iter97.getValue().length);
              for (int _iter98 : _iter97.getValue())
              {
                oprot.writeI32(_iter98);
              }
            }
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, ChordEntities struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(1);
      if (incoming.get(0)) {
        {
          org.apache.thrift.protocol.TMap _map99 = new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.LIST, iprot.readI32());
          struct.entities = new HashMap<String, int[]>(2*_map99.size);
          for (int _i100 = 0; _i100 < _map99.size; ++_i100)
          {
            String _key101;
            int[] _val102;
            _key101 = iprot.readString();
            {
              org.apache.thrift.protocol.TList _list103 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.I32, iprot.readI32());
              _val102 = new int[_list103.size];
              for (int _i104 = 0; _i104 < _list103.size; ++_i104)
              {
                int _elem105;
                _elem105 = iprot.readI32();
                _val102[_i104] = _elem105;
              }
            }
            struct.entities.put(_key101, _val102);
          }
        }
        struct.setEntitiesIsSet(true);
      }
    }
  }

}

