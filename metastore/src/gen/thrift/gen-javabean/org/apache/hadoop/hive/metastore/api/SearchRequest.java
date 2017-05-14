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
public class SearchRequest implements org.apache.thrift.TBase<SearchRequest, SearchRequest._Fields>, java.io.Serializable, Cloneable, Comparable<SearchRequest> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("SearchRequest");

  private static final org.apache.thrift.protocol.TField QUERY_FIELD_DESC = new org.apache.thrift.protocol.TField("query", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField MAX_RESULTS_FIELD_DESC = new org.apache.thrift.protocol.TField("maxResults", org.apache.thrift.protocol.TType.I64, (short)2);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new SearchRequestStandardSchemeFactory());
    schemes.put(TupleScheme.class, new SearchRequestTupleSchemeFactory());
  }

  private String query; // required
  private long maxResults; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    QUERY((short)1, "query"),
    MAX_RESULTS((short)2, "maxResults");

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
        case 1: // QUERY
          return QUERY;
        case 2: // MAX_RESULTS
          return MAX_RESULTS;
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
  private static final int __MAXRESULTS_ISSET_ID = 0;
  private byte __isset_bitfield = 0;
  private static final _Fields optionals[] = {_Fields.MAX_RESULTS};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.QUERY, new org.apache.thrift.meta_data.FieldMetaData("query", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.MAX_RESULTS, new org.apache.thrift.meta_data.FieldMetaData("maxResults", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(SearchRequest.class, metaDataMap);
  }

  public SearchRequest() {
    this.maxResults = -1L;

  }

  public SearchRequest(
    String query)
  {
    this();
    this.query = query;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public SearchRequest(SearchRequest other) {
    __isset_bitfield = other.__isset_bitfield;
    if (other.isSetQuery()) {
      this.query = other.query;
    }
    this.maxResults = other.maxResults;
  }

  public SearchRequest deepCopy() {
    return new SearchRequest(this);
  }

  @Override
  public void clear() {
    this.query = null;
    this.maxResults = -1L;

  }

  public String getQuery() {
    return this.query;
  }

  public void setQuery(String query) {
    this.query = query;
  }

  public void unsetQuery() {
    this.query = null;
  }

  /** Returns true if field query is set (has been assigned a value) and false otherwise */
  public boolean isSetQuery() {
    return this.query != null;
  }

  public void setQueryIsSet(boolean value) {
    if (!value) {
      this.query = null;
    }
  }

  public long getMaxResults() {
    return this.maxResults;
  }

  public void setMaxResults(long maxResults) {
    this.maxResults = maxResults;
    setMaxResultsIsSet(true);
  }

  public void unsetMaxResults() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __MAXRESULTS_ISSET_ID);
  }

  /** Returns true if field maxResults is set (has been assigned a value) and false otherwise */
  public boolean isSetMaxResults() {
    return EncodingUtils.testBit(__isset_bitfield, __MAXRESULTS_ISSET_ID);
  }

  public void setMaxResultsIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __MAXRESULTS_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case QUERY:
      if (value == null) {
        unsetQuery();
      } else {
        setQuery((String)value);
      }
      break;

    case MAX_RESULTS:
      if (value == null) {
        unsetMaxResults();
      } else {
        setMaxResults((Long)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case QUERY:
      return getQuery();

    case MAX_RESULTS:
      return getMaxResults();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case QUERY:
      return isSetQuery();
    case MAX_RESULTS:
      return isSetMaxResults();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof SearchRequest)
      return this.equals((SearchRequest)that);
    return false;
  }

  public boolean equals(SearchRequest that) {
    if (that == null)
      return false;

    boolean this_present_query = true && this.isSetQuery();
    boolean that_present_query = true && that.isSetQuery();
    if (this_present_query || that_present_query) {
      if (!(this_present_query && that_present_query))
        return false;
      if (!this.query.equals(that.query))
        return false;
    }

    boolean this_present_maxResults = true && this.isSetMaxResults();
    boolean that_present_maxResults = true && that.isSetMaxResults();
    if (this_present_maxResults || that_present_maxResults) {
      if (!(this_present_maxResults && that_present_maxResults))
        return false;
      if (this.maxResults != that.maxResults)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_query = true && (isSetQuery());
    list.add(present_query);
    if (present_query)
      list.add(query);

    boolean present_maxResults = true && (isSetMaxResults());
    list.add(present_maxResults);
    if (present_maxResults)
      list.add(maxResults);

    return list.hashCode();
  }

  @Override
  public int compareTo(SearchRequest other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetQuery()).compareTo(other.isSetQuery());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetQuery()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.query, other.query);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetMaxResults()).compareTo(other.isSetMaxResults());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetMaxResults()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.maxResults, other.maxResults);
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
    StringBuilder sb = new StringBuilder("SearchRequest(");
    boolean first = true;

    sb.append("query:");
    if (this.query == null) {
      sb.append("null");
    } else {
      sb.append(this.query);
    }
    first = false;
    if (isSetMaxResults()) {
      if (!first) sb.append(", ");
      sb.append("maxResults:");
      sb.append(this.maxResults);
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!isSetQuery()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'query' is unset! Struct:" + toString());
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
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class SearchRequestStandardSchemeFactory implements SchemeFactory {
    public SearchRequestStandardScheme getScheme() {
      return new SearchRequestStandardScheme();
    }
  }

  private static class SearchRequestStandardScheme extends StandardScheme<SearchRequest> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, SearchRequest struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // QUERY
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.query = iprot.readString();
              struct.setQueryIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // MAX_RESULTS
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.maxResults = iprot.readI64();
              struct.setMaxResultsIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, SearchRequest struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.query != null) {
        oprot.writeFieldBegin(QUERY_FIELD_DESC);
        oprot.writeString(struct.query);
        oprot.writeFieldEnd();
      }
      if (struct.isSetMaxResults()) {
        oprot.writeFieldBegin(MAX_RESULTS_FIELD_DESC);
        oprot.writeI64(struct.maxResults);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class SearchRequestTupleSchemeFactory implements SchemeFactory {
    public SearchRequestTupleScheme getScheme() {
      return new SearchRequestTupleScheme();
    }
  }

  private static class SearchRequestTupleScheme extends TupleScheme<SearchRequest> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, SearchRequest struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      oprot.writeString(struct.query);
      BitSet optionals = new BitSet();
      if (struct.isSetMaxResults()) {
        optionals.set(0);
      }
      oprot.writeBitSet(optionals, 1);
      if (struct.isSetMaxResults()) {
        oprot.writeI64(struct.maxResults);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, SearchRequest struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      struct.query = iprot.readString();
      struct.setQueryIsSet(true);
      BitSet incoming = iprot.readBitSet(1);
      if (incoming.get(0)) {
        struct.maxResults = iprot.readI64();
        struct.setMaxResultsIsSet(true);
      }
    }
  }

}
