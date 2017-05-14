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
public class LockRequest implements org.apache.thrift.TBase<LockRequest, LockRequest._Fields>, java.io.Serializable, Cloneable, Comparable<LockRequest> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("LockRequest");

  private static final org.apache.thrift.protocol.TField COMPONENT_FIELD_DESC = new org.apache.thrift.protocol.TField("component", org.apache.thrift.protocol.TType.LIST, (short)1);
  private static final org.apache.thrift.protocol.TField TXNID_FIELD_DESC = new org.apache.thrift.protocol.TField("txnid", org.apache.thrift.protocol.TType.I64, (short)2);
  private static final org.apache.thrift.protocol.TField USER_FIELD_DESC = new org.apache.thrift.protocol.TField("user", org.apache.thrift.protocol.TType.STRING, (short)3);
  private static final org.apache.thrift.protocol.TField HOSTNAME_FIELD_DESC = new org.apache.thrift.protocol.TField("hostname", org.apache.thrift.protocol.TType.STRING, (short)4);
  private static final org.apache.thrift.protocol.TField AGENT_INFO_FIELD_DESC = new org.apache.thrift.protocol.TField("agentInfo", org.apache.thrift.protocol.TType.STRING, (short)5);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new LockRequestStandardSchemeFactory());
    schemes.put(TupleScheme.class, new LockRequestTupleSchemeFactory());
  }

  private List<LockComponent> component; // required
  private long txnid; // optional
  private String user; // required
  private String hostname; // required
  private String agentInfo; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    COMPONENT((short)1, "component"),
    TXNID((short)2, "txnid"),
    USER((short)3, "user"),
    HOSTNAME((short)4, "hostname"),
    AGENT_INFO((short)5, "agentInfo");

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
        case 1: // COMPONENT
          return COMPONENT;
        case 2: // TXNID
          return TXNID;
        case 3: // USER
          return USER;
        case 4: // HOSTNAME
          return HOSTNAME;
        case 5: // AGENT_INFO
          return AGENT_INFO;
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
  private static final int __TXNID_ISSET_ID = 0;
  private byte __isset_bitfield = 0;
  private static final _Fields optionals[] = {_Fields.TXNID,_Fields.AGENT_INFO};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.COMPONENT, new org.apache.thrift.meta_data.FieldMetaData("component", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, LockComponent.class))));
    tmpMap.put(_Fields.TXNID, new org.apache.thrift.meta_data.FieldMetaData("txnid", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.USER, new org.apache.thrift.meta_data.FieldMetaData("user", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.HOSTNAME, new org.apache.thrift.meta_data.FieldMetaData("hostname", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.AGENT_INFO, new org.apache.thrift.meta_data.FieldMetaData("agentInfo", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(LockRequest.class, metaDataMap);
  }

  public LockRequest() {
    this.agentInfo = "Unknown";

  }

  public LockRequest(
    List<LockComponent> component,
    String user,
    String hostname)
  {
    this();
    this.component = component;
    this.user = user;
    this.hostname = hostname;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public LockRequest(LockRequest other) {
    __isset_bitfield = other.__isset_bitfield;
    if (other.isSetComponent()) {
      List<LockComponent> __this__component = new ArrayList<LockComponent>(other.component.size());
      for (LockComponent other_element : other.component) {
        __this__component.add(new LockComponent(other_element));
      }
      this.component = __this__component;
    }
    this.txnid = other.txnid;
    if (other.isSetUser()) {
      this.user = other.user;
    }
    if (other.isSetHostname()) {
      this.hostname = other.hostname;
    }
    if (other.isSetAgentInfo()) {
      this.agentInfo = other.agentInfo;
    }
  }

  public LockRequest deepCopy() {
    return new LockRequest(this);
  }

  @Override
  public void clear() {
    this.component = null;
    setTxnidIsSet(false);
    this.txnid = 0;
    this.user = null;
    this.hostname = null;
    this.agentInfo = "Unknown";

  }

  public int getComponentSize() {
    return (this.component == null) ? 0 : this.component.size();
  }

  public java.util.Iterator<LockComponent> getComponentIterator() {
    return (this.component == null) ? null : this.component.iterator();
  }

  public void addToComponent(LockComponent elem) {
    if (this.component == null) {
      this.component = new ArrayList<LockComponent>();
    }
    this.component.add(elem);
  }

  public List<LockComponent> getComponent() {
    return this.component;
  }

  public void setComponent(List<LockComponent> component) {
    this.component = component;
  }

  public void unsetComponent() {
    this.component = null;
  }

  /** Returns true if field component is set (has been assigned a value) and false otherwise */
  public boolean isSetComponent() {
    return this.component != null;
  }

  public void setComponentIsSet(boolean value) {
    if (!value) {
      this.component = null;
    }
  }

  public long getTxnid() {
    return this.txnid;
  }

  public void setTxnid(long txnid) {
    this.txnid = txnid;
    setTxnidIsSet(true);
  }

  public void unsetTxnid() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __TXNID_ISSET_ID);
  }

  /** Returns true if field txnid is set (has been assigned a value) and false otherwise */
  public boolean isSetTxnid() {
    return EncodingUtils.testBit(__isset_bitfield, __TXNID_ISSET_ID);
  }

  public void setTxnidIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __TXNID_ISSET_ID, value);
  }

  public String getUser() {
    return this.user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public void unsetUser() {
    this.user = null;
  }

  /** Returns true if field user is set (has been assigned a value) and false otherwise */
  public boolean isSetUser() {
    return this.user != null;
  }

  public void setUserIsSet(boolean value) {
    if (!value) {
      this.user = null;
    }
  }

  public String getHostname() {
    return this.hostname;
  }

  public void setHostname(String hostname) {
    this.hostname = hostname;
  }

  public void unsetHostname() {
    this.hostname = null;
  }

  /** Returns true if field hostname is set (has been assigned a value) and false otherwise */
  public boolean isSetHostname() {
    return this.hostname != null;
  }

  public void setHostnameIsSet(boolean value) {
    if (!value) {
      this.hostname = null;
    }
  }

  public String getAgentInfo() {
    return this.agentInfo;
  }

  public void setAgentInfo(String agentInfo) {
    this.agentInfo = agentInfo;
  }

  public void unsetAgentInfo() {
    this.agentInfo = null;
  }

  /** Returns true if field agentInfo is set (has been assigned a value) and false otherwise */
  public boolean isSetAgentInfo() {
    return this.agentInfo != null;
  }

  public void setAgentInfoIsSet(boolean value) {
    if (!value) {
      this.agentInfo = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case COMPONENT:
      if (value == null) {
        unsetComponent();
      } else {
        setComponent((List<LockComponent>)value);
      }
      break;

    case TXNID:
      if (value == null) {
        unsetTxnid();
      } else {
        setTxnid((Long)value);
      }
      break;

    case USER:
      if (value == null) {
        unsetUser();
      } else {
        setUser((String)value);
      }
      break;

    case HOSTNAME:
      if (value == null) {
        unsetHostname();
      } else {
        setHostname((String)value);
      }
      break;

    case AGENT_INFO:
      if (value == null) {
        unsetAgentInfo();
      } else {
        setAgentInfo((String)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case COMPONENT:
      return getComponent();

    case TXNID:
      return getTxnid();

    case USER:
      return getUser();

    case HOSTNAME:
      return getHostname();

    case AGENT_INFO:
      return getAgentInfo();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case COMPONENT:
      return isSetComponent();
    case TXNID:
      return isSetTxnid();
    case USER:
      return isSetUser();
    case HOSTNAME:
      return isSetHostname();
    case AGENT_INFO:
      return isSetAgentInfo();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof LockRequest)
      return this.equals((LockRequest)that);
    return false;
  }

  public boolean equals(LockRequest that) {
    if (that == null)
      return false;

    boolean this_present_component = true && this.isSetComponent();
    boolean that_present_component = true && that.isSetComponent();
    if (this_present_component || that_present_component) {
      if (!(this_present_component && that_present_component))
        return false;
      if (!this.component.equals(that.component))
        return false;
    }

    boolean this_present_txnid = true && this.isSetTxnid();
    boolean that_present_txnid = true && that.isSetTxnid();
    if (this_present_txnid || that_present_txnid) {
      if (!(this_present_txnid && that_present_txnid))
        return false;
      if (this.txnid != that.txnid)
        return false;
    }

    boolean this_present_user = true && this.isSetUser();
    boolean that_present_user = true && that.isSetUser();
    if (this_present_user || that_present_user) {
      if (!(this_present_user && that_present_user))
        return false;
      if (!this.user.equals(that.user))
        return false;
    }

    boolean this_present_hostname = true && this.isSetHostname();
    boolean that_present_hostname = true && that.isSetHostname();
    if (this_present_hostname || that_present_hostname) {
      if (!(this_present_hostname && that_present_hostname))
        return false;
      if (!this.hostname.equals(that.hostname))
        return false;
    }

    boolean this_present_agentInfo = true && this.isSetAgentInfo();
    boolean that_present_agentInfo = true && that.isSetAgentInfo();
    if (this_present_agentInfo || that_present_agentInfo) {
      if (!(this_present_agentInfo && that_present_agentInfo))
        return false;
      if (!this.agentInfo.equals(that.agentInfo))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_component = true && (isSetComponent());
    list.add(present_component);
    if (present_component)
      list.add(component);

    boolean present_txnid = true && (isSetTxnid());
    list.add(present_txnid);
    if (present_txnid)
      list.add(txnid);

    boolean present_user = true && (isSetUser());
    list.add(present_user);
    if (present_user)
      list.add(user);

    boolean present_hostname = true && (isSetHostname());
    list.add(present_hostname);
    if (present_hostname)
      list.add(hostname);

    boolean present_agentInfo = true && (isSetAgentInfo());
    list.add(present_agentInfo);
    if (present_agentInfo)
      list.add(agentInfo);

    return list.hashCode();
  }

  @Override
  public int compareTo(LockRequest other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetComponent()).compareTo(other.isSetComponent());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetComponent()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.component, other.component);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetTxnid()).compareTo(other.isSetTxnid());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetTxnid()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.txnid, other.txnid);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetUser()).compareTo(other.isSetUser());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetUser()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.user, other.user);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetHostname()).compareTo(other.isSetHostname());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetHostname()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.hostname, other.hostname);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetAgentInfo()).compareTo(other.isSetAgentInfo());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetAgentInfo()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.agentInfo, other.agentInfo);
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
    StringBuilder sb = new StringBuilder("LockRequest(");
    boolean first = true;

    sb.append("component:");
    if (this.component == null) {
      sb.append("null");
    } else {
      sb.append(this.component);
    }
    first = false;
    if (isSetTxnid()) {
      if (!first) sb.append(", ");
      sb.append("txnid:");
      sb.append(this.txnid);
      first = false;
    }
    if (!first) sb.append(", ");
    sb.append("user:");
    if (this.user == null) {
      sb.append("null");
    } else {
      sb.append(this.user);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("hostname:");
    if (this.hostname == null) {
      sb.append("null");
    } else {
      sb.append(this.hostname);
    }
    first = false;
    if (isSetAgentInfo()) {
      if (!first) sb.append(", ");
      sb.append("agentInfo:");
      if (this.agentInfo == null) {
        sb.append("null");
      } else {
        sb.append(this.agentInfo);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!isSetComponent()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'component' is unset! Struct:" + toString());
    }

    if (!isSetUser()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'user' is unset! Struct:" + toString());
    }

    if (!isSetHostname()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'hostname' is unset! Struct:" + toString());
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

  private static class LockRequestStandardSchemeFactory implements SchemeFactory {
    public LockRequestStandardScheme getScheme() {
      return new LockRequestStandardScheme();
    }
  }

  private static class LockRequestStandardScheme extends StandardScheme<LockRequest> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, LockRequest struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // COMPONENT
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list492 = iprot.readListBegin();
                struct.component = new ArrayList<LockComponent>(_list492.size);
                LockComponent _elem493;
                for (int _i494 = 0; _i494 < _list492.size; ++_i494)
                {
                  _elem493 = new LockComponent();
                  _elem493.read(iprot);
                  struct.component.add(_elem493);
                }
                iprot.readListEnd();
              }
              struct.setComponentIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // TXNID
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.txnid = iprot.readI64();
              struct.setTxnidIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // USER
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.user = iprot.readString();
              struct.setUserIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // HOSTNAME
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.hostname = iprot.readString();
              struct.setHostnameIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 5: // AGENT_INFO
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.agentInfo = iprot.readString();
              struct.setAgentInfoIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, LockRequest struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.component != null) {
        oprot.writeFieldBegin(COMPONENT_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.component.size()));
          for (LockComponent _iter495 : struct.component)
          {
            _iter495.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.isSetTxnid()) {
        oprot.writeFieldBegin(TXNID_FIELD_DESC);
        oprot.writeI64(struct.txnid);
        oprot.writeFieldEnd();
      }
      if (struct.user != null) {
        oprot.writeFieldBegin(USER_FIELD_DESC);
        oprot.writeString(struct.user);
        oprot.writeFieldEnd();
      }
      if (struct.hostname != null) {
        oprot.writeFieldBegin(HOSTNAME_FIELD_DESC);
        oprot.writeString(struct.hostname);
        oprot.writeFieldEnd();
      }
      if (struct.agentInfo != null) {
        if (struct.isSetAgentInfo()) {
          oprot.writeFieldBegin(AGENT_INFO_FIELD_DESC);
          oprot.writeString(struct.agentInfo);
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class LockRequestTupleSchemeFactory implements SchemeFactory {
    public LockRequestTupleScheme getScheme() {
      return new LockRequestTupleScheme();
    }
  }

  private static class LockRequestTupleScheme extends TupleScheme<LockRequest> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, LockRequest struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      {
        oprot.writeI32(struct.component.size());
        for (LockComponent _iter496 : struct.component)
        {
          _iter496.write(oprot);
        }
      }
      oprot.writeString(struct.user);
      oprot.writeString(struct.hostname);
      BitSet optionals = new BitSet();
      if (struct.isSetTxnid()) {
        optionals.set(0);
      }
      if (struct.isSetAgentInfo()) {
        optionals.set(1);
      }
      oprot.writeBitSet(optionals, 2);
      if (struct.isSetTxnid()) {
        oprot.writeI64(struct.txnid);
      }
      if (struct.isSetAgentInfo()) {
        oprot.writeString(struct.agentInfo);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, LockRequest struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      {
        org.apache.thrift.protocol.TList _list497 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
        struct.component = new ArrayList<LockComponent>(_list497.size);
        LockComponent _elem498;
        for (int _i499 = 0; _i499 < _list497.size; ++_i499)
        {
          _elem498 = new LockComponent();
          _elem498.read(iprot);
          struct.component.add(_elem498);
        }
      }
      struct.setComponentIsSet(true);
      struct.user = iprot.readString();
      struct.setUserIsSet(true);
      struct.hostname = iprot.readString();
      struct.setHostnameIsSet(true);
      BitSet incoming = iprot.readBitSet(2);
      if (incoming.get(0)) {
        struct.txnid = iprot.readI64();
        struct.setTxnidIsSet(true);
      }
      if (incoming.get(1)) {
        struct.agentInfo = iprot.readString();
        struct.setAgentInfoIsSet(true);
      }
    }
  }

}

