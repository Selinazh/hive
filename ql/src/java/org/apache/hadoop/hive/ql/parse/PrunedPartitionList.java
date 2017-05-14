/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.parse;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.ppr.PartitionPruner;

/**
 * The list of pruned partitions.
 */
public class PrunedPartitionList {

  public static enum loadTypes{SKIP_LOAD, LAZY_LOAD, LOADED};
  
  /** Source table. */
  private final Table source;

  /** Partitions that either satisfy the partition criteria, or may satisfy it. */
  private Set<Partition> partitions;

  /** partition columns referred by pruner expr */
  private List<String> referred;

  /** Partition-names that either satisfy the partition criteria, or may satisfy it. Useful for metadata-only queries.*/
  private Set<String> partitionNames;

  /** Whether there are partitions in the list that may or may not satisfy the criteria. */
  private boolean hasUnknowns;

  /** how partition objects should be loaded. */
  private loadTypes type;

  public PrunedPartitionList(Table source, Set<Partition> partitions, List<String> referred,
      boolean hasUnknowns) {
    this(source, partitions, referred, null, hasUnknowns, loadTypes.LOADED);
  }

  public PrunedPartitionList(Table source, List<String> referred, Set<String> partitionNames,
      boolean hasUnknowns, loadTypes type) {
    this(source, new HashSet<Partition>(), referred, partitionNames, hasUnknowns, type);
  }

  public PrunedPartitionList(Table source, Set<Partition> partitions, List<String> referred,
      Set<String> partitionNames, boolean hasUnknowns, loadTypes type) {
    this.source = source;
    this.referred = referred;
    this.partitions = partitions;
    this.partitionNames = partitionNames;
    this.hasUnknowns = hasUnknowns;
    this.type = type;
  }

  public Table getSourceTable() {
    return source;
  }

  /**
   * @return partitions
   */
  public Set<Partition> getPartitions() throws SemanticException {
    if (type == loadTypes.LAZY_LOAD) {
      // partition objects have not been loaded yet
      try {
        partitions = PartitionPruner.getAllPartitions(source);
      } catch (HiveException e) {
        throw new SemanticException(e);
      }
      partitionNames = null;
      type = loadTypes.LOADED;
    }
    // return loaded partitions. for SKIP_LOAD will return an empty set.
    return partitions;
  }

  /**
   * @return partition-names
   */
  public Set<String> getPartitionNames() { return partitionNames; }

  /**
   * @return all partitions.
   */
  public List<Partition> getNotDeniedPartns() throws SemanticException {
    return new ArrayList<Partition>(getPartitions());
  }

  /**
   * @return Whether there are unknown partitions in {@link #getPartitions()} result.
   */
  public boolean hasUnknownPartitions() {
    return hasUnknowns;
  }

  public List<String> getReferredPartCols() {
    return referred;
  }
}
