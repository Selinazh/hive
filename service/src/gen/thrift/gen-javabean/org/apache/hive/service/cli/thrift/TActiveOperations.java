/**
 * Autogenerated by Thrift Compiler (0.9.3)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.hive.service.cli.thrift;

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
import javax.annotation.Generated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked"})
@Generated(value = "Autogenerated by Thrift Compiler (0.9.3)", date = "2016-10-20")
public class TActiveOperations implements org.apache.thrift.TBase<TActiveOperations, TActiveOperations._Fields>, java.io.Serializable, Cloneable, Comparable<TActiveOperations> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TActiveOperations");

  private static final org.apache.thrift.protocol.TField ACTIVE_OPERATIONS_FIELD_DESC = new org.apache.thrift.protocol.TField("activeOperations", org.apache.thrift.protocol.TType.LIST, (short)1);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new TActiveOperationsStandardSchemeFactory());
    schemes.put(TupleScheme.class, new TActiveOperationsTupleSchemeFactory());
  }

  private List<TOperationStatus> activeOperations; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    ACTIVE_OPERATIONS((short)1, "activeOperations");

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
        case 1: // ACTIVE_OPERATIONS
          return ACTIVE_OPERATIONS;
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
  private static final _Fields optionals[] = {_Fields.ACTIVE_OPERATIONS};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.ACTIVE_OPERATIONS, new org.apache.thrift.meta_data.FieldMetaData("activeOperations", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TOperationStatus.class))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TActiveOperations.class, metaDataMap);
  }

  public TActiveOperations() {
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TActiveOperations(TActiveOperations other) {
    if (other.isSetActiveOperations()) {
      List<TOperationStatus> __this__activeOperations = new ArrayList<TOperationStatus>(other.activeOperations.size());
      for (TOperationStatus other_element : other.activeOperations) {
        __this__activeOperations.add(new TOperationStatus(other_element));
      }
      this.activeOperations = __this__activeOperations;
    }
  }

  public TActiveOperations deepCopy() {
    return new TActiveOperations(this);
  }

  @Override
  public void clear() {
    this.activeOperations = null;
  }

  public int getActiveOperationsSize() {
    return (this.activeOperations == null) ? 0 : this.activeOperations.size();
  }

  public java.util.Iterator<TOperationStatus> getActiveOperationsIterator() {
    return (this.activeOperations == null) ? null : this.activeOperations.iterator();
  }

  public void addToActiveOperations(TOperationStatus elem) {
    if (this.activeOperations == null) {
      this.activeOperations = new ArrayList<TOperationStatus>();
    }
    this.activeOperations.add(elem);
  }

  public List<TOperationStatus> getActiveOperations() {
    return this.activeOperations;
  }

  public void setActiveOperations(List<TOperationStatus> activeOperations) {
    this.activeOperations = activeOperations;
  }

  public void unsetActiveOperations() {
    this.activeOperations = null;
  }

  /** Returns true if field activeOperations is set (has been assigned a value) and false otherwise */
  public boolean isSetActiveOperations() {
    return this.activeOperations != null;
  }

  public void setActiveOperationsIsSet(boolean value) {
    if (!value) {
      this.activeOperations = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case ACTIVE_OPERATIONS:
      if (value == null) {
        unsetActiveOperations();
      } else {
        setActiveOperations((List<TOperationStatus>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case ACTIVE_OPERATIONS:
      return getActiveOperations();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case ACTIVE_OPERATIONS:
      return isSetActiveOperations();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof TActiveOperations)
      return this.equals((TActiveOperations)that);
    return false;
  }

  public boolean equals(TActiveOperations that) {
    if (that == null)
      return false;

    boolean this_present_activeOperations = true && this.isSetActiveOperations();
    boolean that_present_activeOperations = true && that.isSetActiveOperations();
    if (this_present_activeOperations || that_present_activeOperations) {
      if (!(this_present_activeOperations && that_present_activeOperations))
        return false;
      if (!this.activeOperations.equals(that.activeOperations))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_activeOperations = true && (isSetActiveOperations());
    list.add(present_activeOperations);
    if (present_activeOperations)
      list.add(activeOperations);

    return list.hashCode();
  }

  @Override
  public int compareTo(TActiveOperations other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetActiveOperations()).compareTo(other.isSetActiveOperations());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetActiveOperations()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.activeOperations, other.activeOperations);
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
    StringBuilder sb = new StringBuilder("TActiveOperations(");
    boolean first = true;

    if (isSetActiveOperations()) {
      sb.append("activeOperations:");
      if (this.activeOperations == null) {
        sb.append("null");
      } else {
        sb.append(this.activeOperations);
      }
      first = false;
    }
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

  private static class TActiveOperationsStandardSchemeFactory implements SchemeFactory {
    public TActiveOperationsStandardScheme getScheme() {
      return new TActiveOperationsStandardScheme();
    }
  }

  private static class TActiveOperationsStandardScheme extends StandardScheme<TActiveOperations> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, TActiveOperations struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // ACTIVE_OPERATIONS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list188 = iprot.readListBegin();
                struct.activeOperations = new ArrayList<TOperationStatus>(_list188.size);
                TOperationStatus _elem189;
                for (int _i190 = 0; _i190 < _list188.size; ++_i190)
                {
                  _elem189 = new TOperationStatus();
                  _elem189.read(iprot);
                  struct.activeOperations.add(_elem189);
                }
                iprot.readListEnd();
              }
              struct.setActiveOperationsIsSet(true);
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
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, TActiveOperations struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.activeOperations != null) {
        if (struct.isSetActiveOperations()) {
          oprot.writeFieldBegin(ACTIVE_OPERATIONS_FIELD_DESC);
          {
            oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.activeOperations.size()));
            for (TOperationStatus _iter191 : struct.activeOperations)
            {
              _iter191.write(oprot);
            }
            oprot.writeListEnd();
          }
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class TActiveOperationsTupleSchemeFactory implements SchemeFactory {
    public TActiveOperationsTupleScheme getScheme() {
      return new TActiveOperationsTupleScheme();
    }
  }

  private static class TActiveOperationsTupleScheme extends TupleScheme<TActiveOperations> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TActiveOperations struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetActiveOperations()) {
        optionals.set(0);
      }
      oprot.writeBitSet(optionals, 1);
      if (struct.isSetActiveOperations()) {
        {
          oprot.writeI32(struct.activeOperations.size());
          for (TOperationStatus _iter192 : struct.activeOperations)
          {
            _iter192.write(oprot);
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TActiveOperations struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(1);
      if (incoming.get(0)) {
        {
          org.apache.thrift.protocol.TList _list193 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
          struct.activeOperations = new ArrayList<TOperationStatus>(_list193.size);
          TOperationStatus _elem194;
          for (int _i195 = 0; _i195 < _list193.size; ++_i195)
          {
            _elem194 = new TOperationStatus();
            _elem194.read(iprot);
            struct.activeOperations.add(_elem194);
          }
        }
        struct.setActiveOperationsIsSet(true);
      }
    }
  }

}

