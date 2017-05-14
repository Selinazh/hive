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
public class TResourceStatusList implements org.apache.thrift.TBase<TResourceStatusList, TResourceStatusList._Fields>, java.io.Serializable, Cloneable, Comparable<TResourceStatusList> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TResourceStatusList");

  private static final org.apache.thrift.protocol.TField RESOURCES_CONSUMED_FIELD_DESC = new org.apache.thrift.protocol.TField("resourcesConsumed", org.apache.thrift.protocol.TType.LIST, (short)1);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new TResourceStatusListStandardSchemeFactory());
    schemes.put(TupleScheme.class, new TResourceStatusListTupleSchemeFactory());
  }

  private List<TResourceStatus> resourcesConsumed; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    RESOURCES_CONSUMED((short)1, "resourcesConsumed");

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
        case 1: // RESOURCES_CONSUMED
          return RESOURCES_CONSUMED;
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
  private static final _Fields optionals[] = {_Fields.RESOURCES_CONSUMED};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.RESOURCES_CONSUMED, new org.apache.thrift.meta_data.FieldMetaData("resourcesConsumed", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TResourceStatus.class))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TResourceStatusList.class, metaDataMap);
  }

  public TResourceStatusList() {
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TResourceStatusList(TResourceStatusList other) {
    if (other.isSetResourcesConsumed()) {
      List<TResourceStatus> __this__resourcesConsumed = new ArrayList<TResourceStatus>(other.resourcesConsumed.size());
      for (TResourceStatus other_element : other.resourcesConsumed) {
        __this__resourcesConsumed.add(new TResourceStatus(other_element));
      }
      this.resourcesConsumed = __this__resourcesConsumed;
    }
  }

  public TResourceStatusList deepCopy() {
    return new TResourceStatusList(this);
  }

  @Override
  public void clear() {
    this.resourcesConsumed = null;
  }

  public int getResourcesConsumedSize() {
    return (this.resourcesConsumed == null) ? 0 : this.resourcesConsumed.size();
  }

  public java.util.Iterator<TResourceStatus> getResourcesConsumedIterator() {
    return (this.resourcesConsumed == null) ? null : this.resourcesConsumed.iterator();
  }

  public void addToResourcesConsumed(TResourceStatus elem) {
    if (this.resourcesConsumed == null) {
      this.resourcesConsumed = new ArrayList<TResourceStatus>();
    }
    this.resourcesConsumed.add(elem);
  }

  public List<TResourceStatus> getResourcesConsumed() {
    return this.resourcesConsumed;
  }

  public void setResourcesConsumed(List<TResourceStatus> resourcesConsumed) {
    this.resourcesConsumed = resourcesConsumed;
  }

  public void unsetResourcesConsumed() {
    this.resourcesConsumed = null;
  }

  /** Returns true if field resourcesConsumed is set (has been assigned a value) and false otherwise */
  public boolean isSetResourcesConsumed() {
    return this.resourcesConsumed != null;
  }

  public void setResourcesConsumedIsSet(boolean value) {
    if (!value) {
      this.resourcesConsumed = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case RESOURCES_CONSUMED:
      if (value == null) {
        unsetResourcesConsumed();
      } else {
        setResourcesConsumed((List<TResourceStatus>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case RESOURCES_CONSUMED:
      return getResourcesConsumed();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case RESOURCES_CONSUMED:
      return isSetResourcesConsumed();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof TResourceStatusList)
      return this.equals((TResourceStatusList)that);
    return false;
  }

  public boolean equals(TResourceStatusList that) {
    if (that == null)
      return false;

    boolean this_present_resourcesConsumed = true && this.isSetResourcesConsumed();
    boolean that_present_resourcesConsumed = true && that.isSetResourcesConsumed();
    if (this_present_resourcesConsumed || that_present_resourcesConsumed) {
      if (!(this_present_resourcesConsumed && that_present_resourcesConsumed))
        return false;
      if (!this.resourcesConsumed.equals(that.resourcesConsumed))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_resourcesConsumed = true && (isSetResourcesConsumed());
    list.add(present_resourcesConsumed);
    if (present_resourcesConsumed)
      list.add(resourcesConsumed);

    return list.hashCode();
  }

  @Override
  public int compareTo(TResourceStatusList other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetResourcesConsumed()).compareTo(other.isSetResourcesConsumed());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetResourcesConsumed()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.resourcesConsumed, other.resourcesConsumed);
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
    StringBuilder sb = new StringBuilder("TResourceStatusList(");
    boolean first = true;

    if (isSetResourcesConsumed()) {
      sb.append("resourcesConsumed:");
      if (this.resourcesConsumed == null) {
        sb.append("null");
      } else {
        sb.append(this.resourcesConsumed);
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

  private static class TResourceStatusListStandardSchemeFactory implements SchemeFactory {
    public TResourceStatusListStandardScheme getScheme() {
      return new TResourceStatusListStandardScheme();
    }
  }

  private static class TResourceStatusListStandardScheme extends StandardScheme<TResourceStatusList> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, TResourceStatusList struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // RESOURCES_CONSUMED
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list180 = iprot.readListBegin();
                struct.resourcesConsumed = new ArrayList<TResourceStatus>(_list180.size);
                TResourceStatus _elem181;
                for (int _i182 = 0; _i182 < _list180.size; ++_i182)
                {
                  _elem181 = new TResourceStatus();
                  _elem181.read(iprot);
                  struct.resourcesConsumed.add(_elem181);
                }
                iprot.readListEnd();
              }
              struct.setResourcesConsumedIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, TResourceStatusList struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.resourcesConsumed != null) {
        if (struct.isSetResourcesConsumed()) {
          oprot.writeFieldBegin(RESOURCES_CONSUMED_FIELD_DESC);
          {
            oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.resourcesConsumed.size()));
            for (TResourceStatus _iter183 : struct.resourcesConsumed)
            {
              _iter183.write(oprot);
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

  private static class TResourceStatusListTupleSchemeFactory implements SchemeFactory {
    public TResourceStatusListTupleScheme getScheme() {
      return new TResourceStatusListTupleScheme();
    }
  }

  private static class TResourceStatusListTupleScheme extends TupleScheme<TResourceStatusList> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TResourceStatusList struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetResourcesConsumed()) {
        optionals.set(0);
      }
      oprot.writeBitSet(optionals, 1);
      if (struct.isSetResourcesConsumed()) {
        {
          oprot.writeI32(struct.resourcesConsumed.size());
          for (TResourceStatus _iter184 : struct.resourcesConsumed)
          {
            _iter184.write(oprot);
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TResourceStatusList struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(1);
      if (incoming.get(0)) {
        {
          org.apache.thrift.protocol.TList _list185 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
          struct.resourcesConsumed = new ArrayList<TResourceStatus>(_list185.size);
          TResourceStatus _elem186;
          for (int _i187 = 0; _i187 < _list185.size; ++_i187)
          {
            _elem186 = new TResourceStatus();
            _elem186.read(iprot);
            struct.resourcesConsumed.add(_elem186);
          }
        }
        struct.setResourcesConsumedIsSet(true);
      }
    }
  }

}
