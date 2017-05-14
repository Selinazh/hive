/**
 * Autogenerated by Thrift Compiler (0.9.3)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.hadoop.hive.metastore.api;

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
public class SetPartitionsStatsRequest implements org.apache.thrift.TBase<SetPartitionsStatsRequest, SetPartitionsStatsRequest._Fields>, java.io.Serializable, Cloneable, Comparable<SetPartitionsStatsRequest> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("SetPartitionsStatsRequest");

  private static final org.apache.thrift.protocol.TField COL_STATS_FIELD_DESC = new org.apache.thrift.protocol.TField("colStats", org.apache.thrift.protocol.TType.LIST, (short)1);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new SetPartitionsStatsRequestStandardSchemeFactory());
    schemes.put(TupleScheme.class, new SetPartitionsStatsRequestTupleSchemeFactory());
  }

  private List<ColumnStatistics> colStats; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    COL_STATS((short)1, "colStats");

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
        case 1: // COL_STATS
          return COL_STATS;
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
    tmpMap.put(_Fields.COL_STATS, new org.apache.thrift.meta_data.FieldMetaData("colStats", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, ColumnStatistics.class))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(SetPartitionsStatsRequest.class, metaDataMap);
  }

  public SetPartitionsStatsRequest() {
  }

  public SetPartitionsStatsRequest(
    List<ColumnStatistics> colStats)
  {
    this();
    this.colStats = colStats;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public SetPartitionsStatsRequest(SetPartitionsStatsRequest other) {
    if (other.isSetColStats()) {
      List<ColumnStatistics> __this__colStats = new ArrayList<ColumnStatistics>(other.colStats.size());
      for (ColumnStatistics other_element : other.colStats) {
        __this__colStats.add(new ColumnStatistics(other_element));
      }
      this.colStats = __this__colStats;
    }
  }

  public SetPartitionsStatsRequest deepCopy() {
    return new SetPartitionsStatsRequest(this);
  }

  @Override
  public void clear() {
    this.colStats = null;
  }

  public int getColStatsSize() {
    return (this.colStats == null) ? 0 : this.colStats.size();
  }

  public java.util.Iterator<ColumnStatistics> getColStatsIterator() {
    return (this.colStats == null) ? null : this.colStats.iterator();
  }

  public void addToColStats(ColumnStatistics elem) {
    if (this.colStats == null) {
      this.colStats = new ArrayList<ColumnStatistics>();
    }
    this.colStats.add(elem);
  }

  public List<ColumnStatistics> getColStats() {
    return this.colStats;
  }

  public void setColStats(List<ColumnStatistics> colStats) {
    this.colStats = colStats;
  }

  public void unsetColStats() {
    this.colStats = null;
  }

  /** Returns true if field colStats is set (has been assigned a value) and false otherwise */
  public boolean isSetColStats() {
    return this.colStats != null;
  }

  public void setColStatsIsSet(boolean value) {
    if (!value) {
      this.colStats = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case COL_STATS:
      if (value == null) {
        unsetColStats();
      } else {
        setColStats((List<ColumnStatistics>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case COL_STATS:
      return getColStats();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case COL_STATS:
      return isSetColStats();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof SetPartitionsStatsRequest)
      return this.equals((SetPartitionsStatsRequest)that);
    return false;
  }

  public boolean equals(SetPartitionsStatsRequest that) {
    if (that == null)
      return false;

    boolean this_present_colStats = true && this.isSetColStats();
    boolean that_present_colStats = true && that.isSetColStats();
    if (this_present_colStats || that_present_colStats) {
      if (!(this_present_colStats && that_present_colStats))
        return false;
      if (!this.colStats.equals(that.colStats))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_colStats = true && (isSetColStats());
    list.add(present_colStats);
    if (present_colStats)
      list.add(colStats);

    return list.hashCode();
  }

  @Override
  public int compareTo(SetPartitionsStatsRequest other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetColStats()).compareTo(other.isSetColStats());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetColStats()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.colStats, other.colStats);
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
    StringBuilder sb = new StringBuilder("SetPartitionsStatsRequest(");
    boolean first = true;

    sb.append("colStats:");
    if (this.colStats == null) {
      sb.append("null");
    } else {
      sb.append(this.colStats);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!isSetColStats()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'colStats' is unset! Struct:" + toString());
    }

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

  private static class SetPartitionsStatsRequestStandardSchemeFactory implements SchemeFactory {
    public SetPartitionsStatsRequestStandardScheme getScheme() {
      return new SetPartitionsStatsRequestStandardScheme();
    }
  }

  private static class SetPartitionsStatsRequestStandardScheme extends StandardScheme<SetPartitionsStatsRequest> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, SetPartitionsStatsRequest struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // COL_STATS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list286 = iprot.readListBegin();
                struct.colStats = new ArrayList<ColumnStatistics>(_list286.size);
                ColumnStatistics _elem287;
                for (int _i288 = 0; _i288 < _list286.size; ++_i288)
                {
                  _elem287 = new ColumnStatistics();
                  _elem287.read(iprot);
                  struct.colStats.add(_elem287);
                }
                iprot.readListEnd();
              }
              struct.setColStatsIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, SetPartitionsStatsRequest struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.colStats != null) {
        oprot.writeFieldBegin(COL_STATS_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.colStats.size()));
          for (ColumnStatistics _iter289 : struct.colStats)
          {
            _iter289.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class SetPartitionsStatsRequestTupleSchemeFactory implements SchemeFactory {
    public SetPartitionsStatsRequestTupleScheme getScheme() {
      return new SetPartitionsStatsRequestTupleScheme();
    }
  }

  private static class SetPartitionsStatsRequestTupleScheme extends TupleScheme<SetPartitionsStatsRequest> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, SetPartitionsStatsRequest struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      {
        oprot.writeI32(struct.colStats.size());
        for (ColumnStatistics _iter290 : struct.colStats)
        {
          _iter290.write(oprot);
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, SetPartitionsStatsRequest struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      {
        org.apache.thrift.protocol.TList _list291 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
        struct.colStats = new ArrayList<ColumnStatistics>(_list291.size);
        ColumnStatistics _elem292;
        for (int _i293 = 0; _i293 < _list291.size; ++_i293)
        {
          _elem292 = new ColumnStatistics();
          _elem292.read(iprot);
          struct.colStats.add(_elem292);
        }
      }
      struct.setColStatsIsSet(true);
    }
  }

}
