/**
 * Autogenerated by Thrift Compiler (0.9.3)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.hadoop.hive.ql.plan.api;

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
public class Stage implements org.apache.thrift.TBase<Stage, Stage._Fields>, java.io.Serializable, Cloneable, Comparable<Stage> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("Stage");

  private static final org.apache.thrift.protocol.TField STAGE_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("stageId", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField STAGE_TYPE_FIELD_DESC = new org.apache.thrift.protocol.TField("stageType", org.apache.thrift.protocol.TType.I32, (short)2);
  private static final org.apache.thrift.protocol.TField STAGE_ATTRIBUTES_FIELD_DESC = new org.apache.thrift.protocol.TField("stageAttributes", org.apache.thrift.protocol.TType.MAP, (short)3);
  private static final org.apache.thrift.protocol.TField STAGE_COUNTERS_FIELD_DESC = new org.apache.thrift.protocol.TField("stageCounters", org.apache.thrift.protocol.TType.MAP, (short)4);
  private static final org.apache.thrift.protocol.TField TASK_LIST_FIELD_DESC = new org.apache.thrift.protocol.TField("taskList", org.apache.thrift.protocol.TType.LIST, (short)5);
  private static final org.apache.thrift.protocol.TField DONE_FIELD_DESC = new org.apache.thrift.protocol.TField("done", org.apache.thrift.protocol.TType.BOOL, (short)6);
  private static final org.apache.thrift.protocol.TField STARTED_FIELD_DESC = new org.apache.thrift.protocol.TField("started", org.apache.thrift.protocol.TType.BOOL, (short)7);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new StageStandardSchemeFactory());
    schemes.put(TupleScheme.class, new StageTupleSchemeFactory());
  }

  private String stageId; // required
  private StageType stageType; // required
  private Map<String,String> stageAttributes; // required
  private Map<String,Long> stageCounters; // required
  private List<Task> taskList; // required
  private boolean done; // required
  private boolean started; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    STAGE_ID((short)1, "stageId"),
    /**
     * 
     * @see StageType
     */
    STAGE_TYPE((short)2, "stageType"),
    STAGE_ATTRIBUTES((short)3, "stageAttributes"),
    STAGE_COUNTERS((short)4, "stageCounters"),
    TASK_LIST((short)5, "taskList"),
    DONE((short)6, "done"),
    STARTED((short)7, "started");

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
        case 1: // STAGE_ID
          return STAGE_ID;
        case 2: // STAGE_TYPE
          return STAGE_TYPE;
        case 3: // STAGE_ATTRIBUTES
          return STAGE_ATTRIBUTES;
        case 4: // STAGE_COUNTERS
          return STAGE_COUNTERS;
        case 5: // TASK_LIST
          return TASK_LIST;
        case 6: // DONE
          return DONE;
        case 7: // STARTED
          return STARTED;
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
  private static final int __DONE_ISSET_ID = 0;
  private static final int __STARTED_ISSET_ID = 1;
  private byte __isset_bitfield = 0;
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.STAGE_ID, new org.apache.thrift.meta_data.FieldMetaData("stageId", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.STAGE_TYPE, new org.apache.thrift.meta_data.FieldMetaData("stageType", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.EnumMetaData(org.apache.thrift.protocol.TType.ENUM, StageType.class)));
    tmpMap.put(_Fields.STAGE_ATTRIBUTES, new org.apache.thrift.meta_data.FieldMetaData("stageAttributes", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING), 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING))));
    tmpMap.put(_Fields.STAGE_COUNTERS, new org.apache.thrift.meta_data.FieldMetaData("stageCounters", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING), 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64))));
    tmpMap.put(_Fields.TASK_LIST, new org.apache.thrift.meta_data.FieldMetaData("taskList", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, Task.class))));
    tmpMap.put(_Fields.DONE, new org.apache.thrift.meta_data.FieldMetaData("done", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.BOOL)));
    tmpMap.put(_Fields.STARTED, new org.apache.thrift.meta_data.FieldMetaData("started", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.BOOL)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(Stage.class, metaDataMap);
  }

  public Stage() {
  }

  public Stage(
    String stageId,
    StageType stageType,
    Map<String,String> stageAttributes,
    Map<String,Long> stageCounters,
    List<Task> taskList,
    boolean done,
    boolean started)
  {
    this();
    this.stageId = stageId;
    this.stageType = stageType;
    this.stageAttributes = stageAttributes;
    this.stageCounters = stageCounters;
    this.taskList = taskList;
    this.done = done;
    setDoneIsSet(true);
    this.started = started;
    setStartedIsSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public Stage(Stage other) {
    __isset_bitfield = other.__isset_bitfield;
    if (other.isSetStageId()) {
      this.stageId = other.stageId;
    }
    if (other.isSetStageType()) {
      this.stageType = other.stageType;
    }
    if (other.isSetStageAttributes()) {
      Map<String,String> __this__stageAttributes = new HashMap<String,String>(other.stageAttributes);
      this.stageAttributes = __this__stageAttributes;
    }
    if (other.isSetStageCounters()) {
      Map<String,Long> __this__stageCounters = new HashMap<String,Long>(other.stageCounters);
      this.stageCounters = __this__stageCounters;
    }
    if (other.isSetTaskList()) {
      List<Task> __this__taskList = new ArrayList<Task>(other.taskList.size());
      for (Task other_element : other.taskList) {
        __this__taskList.add(new Task(other_element));
      }
      this.taskList = __this__taskList;
    }
    this.done = other.done;
    this.started = other.started;
  }

  public Stage deepCopy() {
    return new Stage(this);
  }

  @Override
  public void clear() {
    this.stageId = null;
    this.stageType = null;
    this.stageAttributes = null;
    this.stageCounters = null;
    this.taskList = null;
    setDoneIsSet(false);
    this.done = false;
    setStartedIsSet(false);
    this.started = false;
  }

  public String getStageId() {
    return this.stageId;
  }

  public void setStageId(String stageId) {
    this.stageId = stageId;
  }

  public void unsetStageId() {
    this.stageId = null;
  }

  /** Returns true if field stageId is set (has been assigned a value) and false otherwise */
  public boolean isSetStageId() {
    return this.stageId != null;
  }

  public void setStageIdIsSet(boolean value) {
    if (!value) {
      this.stageId = null;
    }
  }

  /**
   * 
   * @see StageType
   */
  public StageType getStageType() {
    return this.stageType;
  }

  /**
   * 
   * @see StageType
   */
  public void setStageType(StageType stageType) {
    this.stageType = stageType;
  }

  public void unsetStageType() {
    this.stageType = null;
  }

  /** Returns true if field stageType is set (has been assigned a value) and false otherwise */
  public boolean isSetStageType() {
    return this.stageType != null;
  }

  public void setStageTypeIsSet(boolean value) {
    if (!value) {
      this.stageType = null;
    }
  }

  public int getStageAttributesSize() {
    return (this.stageAttributes == null) ? 0 : this.stageAttributes.size();
  }

  public void putToStageAttributes(String key, String val) {
    if (this.stageAttributes == null) {
      this.stageAttributes = new HashMap<String,String>();
    }
    this.stageAttributes.put(key, val);
  }

  public Map<String,String> getStageAttributes() {
    return this.stageAttributes;
  }

  public void setStageAttributes(Map<String,String> stageAttributes) {
    this.stageAttributes = stageAttributes;
  }

  public void unsetStageAttributes() {
    this.stageAttributes = null;
  }

  /** Returns true if field stageAttributes is set (has been assigned a value) and false otherwise */
  public boolean isSetStageAttributes() {
    return this.stageAttributes != null;
  }

  public void setStageAttributesIsSet(boolean value) {
    if (!value) {
      this.stageAttributes = null;
    }
  }

  public int getStageCountersSize() {
    return (this.stageCounters == null) ? 0 : this.stageCounters.size();
  }

  public void putToStageCounters(String key, long val) {
    if (this.stageCounters == null) {
      this.stageCounters = new HashMap<String,Long>();
    }
    this.stageCounters.put(key, val);
  }

  public Map<String,Long> getStageCounters() {
    return this.stageCounters;
  }

  public void setStageCounters(Map<String,Long> stageCounters) {
    this.stageCounters = stageCounters;
  }

  public void unsetStageCounters() {
    this.stageCounters = null;
  }

  /** Returns true if field stageCounters is set (has been assigned a value) and false otherwise */
  public boolean isSetStageCounters() {
    return this.stageCounters != null;
  }

  public void setStageCountersIsSet(boolean value) {
    if (!value) {
      this.stageCounters = null;
    }
  }

  public int getTaskListSize() {
    return (this.taskList == null) ? 0 : this.taskList.size();
  }

  public java.util.Iterator<Task> getTaskListIterator() {
    return (this.taskList == null) ? null : this.taskList.iterator();
  }

  public void addToTaskList(Task elem) {
    if (this.taskList == null) {
      this.taskList = new ArrayList<Task>();
    }
    this.taskList.add(elem);
  }

  public List<Task> getTaskList() {
    return this.taskList;
  }

  public void setTaskList(List<Task> taskList) {
    this.taskList = taskList;
  }

  public void unsetTaskList() {
    this.taskList = null;
  }

  /** Returns true if field taskList is set (has been assigned a value) and false otherwise */
  public boolean isSetTaskList() {
    return this.taskList != null;
  }

  public void setTaskListIsSet(boolean value) {
    if (!value) {
      this.taskList = null;
    }
  }

  public boolean isDone() {
    return this.done;
  }

  public void setDone(boolean done) {
    this.done = done;
    setDoneIsSet(true);
  }

  public void unsetDone() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __DONE_ISSET_ID);
  }

  /** Returns true if field done is set (has been assigned a value) and false otherwise */
  public boolean isSetDone() {
    return EncodingUtils.testBit(__isset_bitfield, __DONE_ISSET_ID);
  }

  public void setDoneIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __DONE_ISSET_ID, value);
  }

  public boolean isStarted() {
    return this.started;
  }

  public void setStarted(boolean started) {
    this.started = started;
    setStartedIsSet(true);
  }

  public void unsetStarted() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __STARTED_ISSET_ID);
  }

  /** Returns true if field started is set (has been assigned a value) and false otherwise */
  public boolean isSetStarted() {
    return EncodingUtils.testBit(__isset_bitfield, __STARTED_ISSET_ID);
  }

  public void setStartedIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __STARTED_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case STAGE_ID:
      if (value == null) {
        unsetStageId();
      } else {
        setStageId((String)value);
      }
      break;

    case STAGE_TYPE:
      if (value == null) {
        unsetStageType();
      } else {
        setStageType((StageType)value);
      }
      break;

    case STAGE_ATTRIBUTES:
      if (value == null) {
        unsetStageAttributes();
      } else {
        setStageAttributes((Map<String,String>)value);
      }
      break;

    case STAGE_COUNTERS:
      if (value == null) {
        unsetStageCounters();
      } else {
        setStageCounters((Map<String,Long>)value);
      }
      break;

    case TASK_LIST:
      if (value == null) {
        unsetTaskList();
      } else {
        setTaskList((List<Task>)value);
      }
      break;

    case DONE:
      if (value == null) {
        unsetDone();
      } else {
        setDone((Boolean)value);
      }
      break;

    case STARTED:
      if (value == null) {
        unsetStarted();
      } else {
        setStarted((Boolean)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case STAGE_ID:
      return getStageId();

    case STAGE_TYPE:
      return getStageType();

    case STAGE_ATTRIBUTES:
      return getStageAttributes();

    case STAGE_COUNTERS:
      return getStageCounters();

    case TASK_LIST:
      return getTaskList();

    case DONE:
      return isDone();

    case STARTED:
      return isStarted();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case STAGE_ID:
      return isSetStageId();
    case STAGE_TYPE:
      return isSetStageType();
    case STAGE_ATTRIBUTES:
      return isSetStageAttributes();
    case STAGE_COUNTERS:
      return isSetStageCounters();
    case TASK_LIST:
      return isSetTaskList();
    case DONE:
      return isSetDone();
    case STARTED:
      return isSetStarted();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof Stage)
      return this.equals((Stage)that);
    return false;
  }

  public boolean equals(Stage that) {
    if (that == null)
      return false;

    boolean this_present_stageId = true && this.isSetStageId();
    boolean that_present_stageId = true && that.isSetStageId();
    if (this_present_stageId || that_present_stageId) {
      if (!(this_present_stageId && that_present_stageId))
        return false;
      if (!this.stageId.equals(that.stageId))
        return false;
    }

    boolean this_present_stageType = true && this.isSetStageType();
    boolean that_present_stageType = true && that.isSetStageType();
    if (this_present_stageType || that_present_stageType) {
      if (!(this_present_stageType && that_present_stageType))
        return false;
      if (!this.stageType.equals(that.stageType))
        return false;
    }

    boolean this_present_stageAttributes = true && this.isSetStageAttributes();
    boolean that_present_stageAttributes = true && that.isSetStageAttributes();
    if (this_present_stageAttributes || that_present_stageAttributes) {
      if (!(this_present_stageAttributes && that_present_stageAttributes))
        return false;
      if (!this.stageAttributes.equals(that.stageAttributes))
        return false;
    }

    boolean this_present_stageCounters = true && this.isSetStageCounters();
    boolean that_present_stageCounters = true && that.isSetStageCounters();
    if (this_present_stageCounters || that_present_stageCounters) {
      if (!(this_present_stageCounters && that_present_stageCounters))
        return false;
      if (!this.stageCounters.equals(that.stageCounters))
        return false;
    }

    boolean this_present_taskList = true && this.isSetTaskList();
    boolean that_present_taskList = true && that.isSetTaskList();
    if (this_present_taskList || that_present_taskList) {
      if (!(this_present_taskList && that_present_taskList))
        return false;
      if (!this.taskList.equals(that.taskList))
        return false;
    }

    boolean this_present_done = true;
    boolean that_present_done = true;
    if (this_present_done || that_present_done) {
      if (!(this_present_done && that_present_done))
        return false;
      if (this.done != that.done)
        return false;
    }

    boolean this_present_started = true;
    boolean that_present_started = true;
    if (this_present_started || that_present_started) {
      if (!(this_present_started && that_present_started))
        return false;
      if (this.started != that.started)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_stageId = true && (isSetStageId());
    list.add(present_stageId);
    if (present_stageId)
      list.add(stageId);

    boolean present_stageType = true && (isSetStageType());
    list.add(present_stageType);
    if (present_stageType)
      list.add(stageType.getValue());

    boolean present_stageAttributes = true && (isSetStageAttributes());
    list.add(present_stageAttributes);
    if (present_stageAttributes)
      list.add(stageAttributes);

    boolean present_stageCounters = true && (isSetStageCounters());
    list.add(present_stageCounters);
    if (present_stageCounters)
      list.add(stageCounters);

    boolean present_taskList = true && (isSetTaskList());
    list.add(present_taskList);
    if (present_taskList)
      list.add(taskList);

    boolean present_done = true;
    list.add(present_done);
    if (present_done)
      list.add(done);

    boolean present_started = true;
    list.add(present_started);
    if (present_started)
      list.add(started);

    return list.hashCode();
  }

  @Override
  public int compareTo(Stage other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetStageId()).compareTo(other.isSetStageId());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStageId()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.stageId, other.stageId);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetStageType()).compareTo(other.isSetStageType());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStageType()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.stageType, other.stageType);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetStageAttributes()).compareTo(other.isSetStageAttributes());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStageAttributes()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.stageAttributes, other.stageAttributes);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetStageCounters()).compareTo(other.isSetStageCounters());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStageCounters()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.stageCounters, other.stageCounters);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetTaskList()).compareTo(other.isSetTaskList());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetTaskList()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.taskList, other.taskList);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetDone()).compareTo(other.isSetDone());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetDone()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.done, other.done);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetStarted()).compareTo(other.isSetStarted());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStarted()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.started, other.started);
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
    StringBuilder sb = new StringBuilder("Stage(");
    boolean first = true;

    sb.append("stageId:");
    if (this.stageId == null) {
      sb.append("null");
    } else {
      sb.append(this.stageId);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("stageType:");
    if (this.stageType == null) {
      sb.append("null");
    } else {
      sb.append(this.stageType);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("stageAttributes:");
    if (this.stageAttributes == null) {
      sb.append("null");
    } else {
      sb.append(this.stageAttributes);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("stageCounters:");
    if (this.stageCounters == null) {
      sb.append("null");
    } else {
      sb.append(this.stageCounters);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("taskList:");
    if (this.taskList == null) {
      sb.append("null");
    } else {
      sb.append(this.taskList);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("done:");
    sb.append(this.done);
    first = false;
    if (!first) sb.append(", ");
    sb.append("started:");
    sb.append(this.started);
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
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class StageStandardSchemeFactory implements SchemeFactory {
    public StageStandardScheme getScheme() {
      return new StageStandardScheme();
    }
  }

  private static class StageStandardScheme extends StandardScheme<Stage> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, Stage struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // STAGE_ID
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.stageId = iprot.readString();
              struct.setStageIdIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // STAGE_TYPE
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.stageType = org.apache.hadoop.hive.ql.plan.api.StageType.findByValue(iprot.readI32());
              struct.setStageTypeIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // STAGE_ATTRIBUTES
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map72 = iprot.readMapBegin();
                struct.stageAttributes = new HashMap<String,String>(2*_map72.size);
                String _key73;
                String _val74;
                for (int _i75 = 0; _i75 < _map72.size; ++_i75)
                {
                  _key73 = iprot.readString();
                  _val74 = iprot.readString();
                  struct.stageAttributes.put(_key73, _val74);
                }
                iprot.readMapEnd();
              }
              struct.setStageAttributesIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // STAGE_COUNTERS
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map76 = iprot.readMapBegin();
                struct.stageCounters = new HashMap<String,Long>(2*_map76.size);
                String _key77;
                long _val78;
                for (int _i79 = 0; _i79 < _map76.size; ++_i79)
                {
                  _key77 = iprot.readString();
                  _val78 = iprot.readI64();
                  struct.stageCounters.put(_key77, _val78);
                }
                iprot.readMapEnd();
              }
              struct.setStageCountersIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 5: // TASK_LIST
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list80 = iprot.readListBegin();
                struct.taskList = new ArrayList<Task>(_list80.size);
                Task _elem81;
                for (int _i82 = 0; _i82 < _list80.size; ++_i82)
                {
                  _elem81 = new Task();
                  _elem81.read(iprot);
                  struct.taskList.add(_elem81);
                }
                iprot.readListEnd();
              }
              struct.setTaskListIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 6: // DONE
            if (schemeField.type == org.apache.thrift.protocol.TType.BOOL) {
              struct.done = iprot.readBool();
              struct.setDoneIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 7: // STARTED
            if (schemeField.type == org.apache.thrift.protocol.TType.BOOL) {
              struct.started = iprot.readBool();
              struct.setStartedIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, Stage struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.stageId != null) {
        oprot.writeFieldBegin(STAGE_ID_FIELD_DESC);
        oprot.writeString(struct.stageId);
        oprot.writeFieldEnd();
      }
      if (struct.stageType != null) {
        oprot.writeFieldBegin(STAGE_TYPE_FIELD_DESC);
        oprot.writeI32(struct.stageType.getValue());
        oprot.writeFieldEnd();
      }
      if (struct.stageAttributes != null) {
        oprot.writeFieldBegin(STAGE_ATTRIBUTES_FIELD_DESC);
        {
          oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRING, struct.stageAttributes.size()));
          for (Map.Entry<String, String> _iter83 : struct.stageAttributes.entrySet())
          {
            oprot.writeString(_iter83.getKey());
            oprot.writeString(_iter83.getValue());
          }
          oprot.writeMapEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.stageCounters != null) {
        oprot.writeFieldBegin(STAGE_COUNTERS_FIELD_DESC);
        {
          oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.I64, struct.stageCounters.size()));
          for (Map.Entry<String, Long> _iter84 : struct.stageCounters.entrySet())
          {
            oprot.writeString(_iter84.getKey());
            oprot.writeI64(_iter84.getValue());
          }
          oprot.writeMapEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.taskList != null) {
        oprot.writeFieldBegin(TASK_LIST_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.taskList.size()));
          for (Task _iter85 : struct.taskList)
          {
            _iter85.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(DONE_FIELD_DESC);
      oprot.writeBool(struct.done);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(STARTED_FIELD_DESC);
      oprot.writeBool(struct.started);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class StageTupleSchemeFactory implements SchemeFactory {
    public StageTupleScheme getScheme() {
      return new StageTupleScheme();
    }
  }

  private static class StageTupleScheme extends TupleScheme<Stage> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, Stage struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetStageId()) {
        optionals.set(0);
      }
      if (struct.isSetStageType()) {
        optionals.set(1);
      }
      if (struct.isSetStageAttributes()) {
        optionals.set(2);
      }
      if (struct.isSetStageCounters()) {
        optionals.set(3);
      }
      if (struct.isSetTaskList()) {
        optionals.set(4);
      }
      if (struct.isSetDone()) {
        optionals.set(5);
      }
      if (struct.isSetStarted()) {
        optionals.set(6);
      }
      oprot.writeBitSet(optionals, 7);
      if (struct.isSetStageId()) {
        oprot.writeString(struct.stageId);
      }
      if (struct.isSetStageType()) {
        oprot.writeI32(struct.stageType.getValue());
      }
      if (struct.isSetStageAttributes()) {
        {
          oprot.writeI32(struct.stageAttributes.size());
          for (Map.Entry<String, String> _iter86 : struct.stageAttributes.entrySet())
          {
            oprot.writeString(_iter86.getKey());
            oprot.writeString(_iter86.getValue());
          }
        }
      }
      if (struct.isSetStageCounters()) {
        {
          oprot.writeI32(struct.stageCounters.size());
          for (Map.Entry<String, Long> _iter87 : struct.stageCounters.entrySet())
          {
            oprot.writeString(_iter87.getKey());
            oprot.writeI64(_iter87.getValue());
          }
        }
      }
      if (struct.isSetTaskList()) {
        {
          oprot.writeI32(struct.taskList.size());
          for (Task _iter88 : struct.taskList)
          {
            _iter88.write(oprot);
          }
        }
      }
      if (struct.isSetDone()) {
        oprot.writeBool(struct.done);
      }
      if (struct.isSetStarted()) {
        oprot.writeBool(struct.started);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, Stage struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(7);
      if (incoming.get(0)) {
        struct.stageId = iprot.readString();
        struct.setStageIdIsSet(true);
      }
      if (incoming.get(1)) {
        struct.stageType = org.apache.hadoop.hive.ql.plan.api.StageType.findByValue(iprot.readI32());
        struct.setStageTypeIsSet(true);
      }
      if (incoming.get(2)) {
        {
          org.apache.thrift.protocol.TMap _map89 = new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRING, iprot.readI32());
          struct.stageAttributes = new HashMap<String,String>(2*_map89.size);
          String _key90;
          String _val91;
          for (int _i92 = 0; _i92 < _map89.size; ++_i92)
          {
            _key90 = iprot.readString();
            _val91 = iprot.readString();
            struct.stageAttributes.put(_key90, _val91);
          }
        }
        struct.setStageAttributesIsSet(true);
      }
      if (incoming.get(3)) {
        {
          org.apache.thrift.protocol.TMap _map93 = new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.I64, iprot.readI32());
          struct.stageCounters = new HashMap<String,Long>(2*_map93.size);
          String _key94;
          long _val95;
          for (int _i96 = 0; _i96 < _map93.size; ++_i96)
          {
            _key94 = iprot.readString();
            _val95 = iprot.readI64();
            struct.stageCounters.put(_key94, _val95);
          }
        }
        struct.setStageCountersIsSet(true);
      }
      if (incoming.get(4)) {
        {
          org.apache.thrift.protocol.TList _list97 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
          struct.taskList = new ArrayList<Task>(_list97.size);
          Task _elem98;
          for (int _i99 = 0; _i99 < _list97.size; ++_i99)
          {
            _elem98 = new Task();
            _elem98.read(iprot);
            struct.taskList.add(_elem98);
          }
        }
        struct.setTaskListIsSet(true);
      }
      if (incoming.get(5)) {
        struct.done = iprot.readBool();
        struct.setDoneIsSet(true);
      }
      if (incoming.get(6)) {
        struct.started = iprot.readBool();
        struct.setStartedIsSet(true);
      }
    }
  }

}

