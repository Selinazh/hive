package org.apache.hadoop.hive.ql.optimizer.physical;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.NullStructSerDe.NullStructSerDeObjectInspector;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.mapred.TextInputFormat;


public class MetadataOnlyTaskDispatcher extends NullScanTaskDispatcher {
  public MetadataOnlyTaskDispatcher(PhysicalContext context, Map<Rule, NodeProcessor> rules) {
    super(context, rules);
  }

  @Override
  protected void processAlias(MapWork work, Set<TableScanOperator> operators) {
    Set<TableScanOperator> candidates = new HashSet<TableScanOperator>();
    candidates.addAll(operators);
    for (TableScanOperator operator : operators) {
      if (work.getPartNames().get(operator)!= null
          && work.getPartNames().get(operator).size() > 0){
        generateLocalWork(work, operator);
        candidates.remove(operator);
      }
    }
    if (candidates.size() > 0) {
      super.processAlias(work, candidates);
    }
  }
  
  private PartitionDesc changePartitionToMetadataOnly(PartitionDesc desc) {
    if (desc != null) {
      desc.getTableDesc().setInputFileFormatClass(TextInputFormat.class);
      desc.getTableDesc().setOutputFileFormatClass(HiveIgnoreKeyTextOutputFormat.class);
      desc.getTableDesc().getProperties().setProperty(serdeConstants.SERIALIZATION_LIB,
          LazySimpleSerDe.class.getName());
      desc.setInputFileFormatClass(TextInputFormat.class);
      desc.setOutputFileFormatClass(HiveIgnoreKeyTextOutputFormat.class);
      desc.getProperties().setProperty(serdeConstants.SERIALIZATION_LIB,
          LazySimpleSerDe.class.getName());
    }
    return desc;
  }

  // generate local execution work for metadata only task
  private void generateLocalWork(MapWork work, TableScanOperator tso) {
    PhysicalContext context = getPhysicalContext();
    // assuming partition keys are not changeable - using partition column info
    // stored in table level.
    List<Object[]> rows = new ArrayList<Object[]>();
    List<StructObjectInspector> rowInspectors = new ArrayList<StructObjectInspector>();
    Properties props = tso.getConf().getTableMetadata().getMetadata();
    String pcols = props.getProperty(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS);
    String pcolTypes = props.getProperty(hive_metastoreConstants.META_TABLE_PARTITION_COLUMN_TYPES);
    String[] partKeys = pcols.trim().split("/");
    String[] partKeyTypes = pcolTypes.trim().split(":");
    // only partition names are available
    createPartitionKeyValuesFromPartitionNames(work, rows, rowInspectors, partKeys, partKeyTypes, tso);
 
    // add partition key values to tso
    tso.rowInspectors.addAll(rowInspectors);
    tso.rows.addAll(rows);
    // remove partition paths, put temp path instead
    String alias = getAliasForTableScanOperator(work, tso);
    PartitionDesc aliasPartn = work.getAliasToPartnInfo().get(alias);
    changePartitionToMetadataOnly(aliasPartn);
    String tmpPath = context.getContext().getMRTmpPath() + aliasPartn.getTableName()
        + aliasPartn.hashCode();
    ArrayList<String> aliases = new ArrayList<String>();
    aliases.add(alias);
    work.getPathToAliases().put(tmpPath, aliases);
    work.getPathToPartitionInfo().put(tmpPath, aliasPartn);
    work.getAliasToPartnInfo().put(alias, aliasPartn);
    // a workaround for self join because TableDesc is global cached.
    TableDesc newTblDesc = (TableDesc) work.getAliasToPartnInfo().get(alias).getTableDesc().clone();
    Properties prop = newTblDesc.getProperties();
    prop.setProperty(serdeConstants.SERIALIZATION_DDL,
        getDDLFromFieldSchema(getAliasForTableScanOperator(work, tso), partKeys, partKeyTypes));
    prop.setProperty(serdeConstants.LIST_COLUMN_TYPES, getColumns(partKeyTypes, ":"));
    prop.setProperty(serdeConstants.LIST_COLUMNS, getColumns(partKeys, ","));
    prop.setProperty(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, "");
    prop.setProperty(hive_metastoreConstants.META_TABLE_PARTITION_COLUMN_TYPES, "");
    prop.setProperty(serdeConstants.SERIALIZATION_CLASS, LazySimpleSerDe.class.getName());
    work.getAliasToPartnInfo().get(alias).setTableDesc(newTblDesc);

    LOG.info("generated local execution plan for metadata only table scan "
        + getAliasForTableScanOperator(work, tso));
  }

  private void createPartitionKeyValuesFromPartitionNames(MapWork work, List<Object[]> rows,
      List<StructObjectInspector> rowInspectors, String[] partKeys, String[] partKeyTypes, TableScanOperator tso)
      {
    NullStructSerDeObjectInspector serde = new NullStructSerDeObjectInspector();

    for (String partName : work.getPartNames().get(tso)) {
      Map<String, String> partKeyValue;
      try {
        partKeyValue = Warehouse.makeSpecFromName(partName);
      } catch (MetaException e) {
        // not possible
        return;
      }
      List<Object> objectVals = new ArrayList<Object>();
      for (int i = 0; i < partKeys.length; i++) {
        String key = partKeys[i];
        String value = partKeyValue.get(key);
        ObjectInspector objectInspector = null;
        // Create a Standard java object Inspector
        objectInspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(TypeInfoFactory
            .getPrimitiveTypeInfo(partKeyTypes[i]));
        objectVals.add(ObjectInspectorConverters.getConverter(
            PrimitiveObjectInspectorFactory.javaStringObjectInspector, objectInspector).convert(
            value));
      }
      Object[] row = new Object[2];
      row[1] = null;
      row[0] = objectVals;
      InspectableObject inspectable = new InspectableObject();
      inspectable.o = row;
      inspectable.oi = serde;
      rowInspectors.add(createRowInspector(serde, partKeys, partKeyTypes));
      rows.add(row);
    }
  }

  private static String getDDLFromFieldSchema(String structName, String[] partKeys,
      String[] partKeyTypes) {
    StringBuilder ddl = new StringBuilder();
    ddl.append("struct ");
    ddl.append(structName);
    ddl.append(" { ");
    boolean first = true;
    for (int i = 0; i < partKeys.length; i++) {
      String col = partKeys[i];
      if (first) {
        first = false;
      } else {
        ddl.append(", ");
      }
      String type = partKeyTypes[i];
      ddl.append(MetaStoreUtils.typeToThriftType(type));
      ddl.append(' ');
      ddl.append(col);
    }
    ddl.append("}");
    LOG.debug("DDL: " + ddl);
    return ddl.toString();
  }

  private static String getColumns(String[] partKeyTypes, String delimiter) {
    StringBuilder sb = new StringBuilder();
    for (String type : partKeyTypes) {
      sb.append(type);
      sb.append(delimiter);
    }
    return sb.toString().substring(0, sb.length() - 1);
  }

  private static StructObjectInspector createRowInspector(StructObjectInspector current,
      String[] partKeys, String[] partKeyTypes) {
    List<String> partNames = new ArrayList<String>();
    List<ObjectInspector> partObjectInspectors = new ArrayList<ObjectInspector>();
    for (int i = 0; i < partKeys.length; i++) {
      String key = partKeys[i];
      partNames.add(key);
      ObjectInspector oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(TypeInfoFactory
          .getPrimitiveTypeInfo(partKeyTypes[i]));
      partObjectInspectors.add(oi);
    }
    StructObjectInspector partObjectInspector = ObjectInspectorFactory
        .getStandardStructObjectInspector(partNames, partObjectInspectors);
    return ObjectInspectorFactory.getUnionStructObjectInspector(Arrays.asList(partObjectInspector,
        current));
  }

}
