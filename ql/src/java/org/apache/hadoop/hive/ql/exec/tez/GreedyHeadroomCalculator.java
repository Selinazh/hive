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

package org.apache.hadoop.hive.ql.exec.tez;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tez.runtime.api.InputInitializerContext;

class GreedyHeadroomCalculator extends QueueHeadroomCalculator {

  private static final Log LOG = LogFactory.getLog(HiveSplitGenerator.class);

  @Override
  public int getAvailableSlots(InputInitializerContext context) {
    int totalResource = context.getTotalAvailableResource().getMemory();
    int taskResource = context.getVertexTaskResource().getMemory();
    int availableSlots = totalResource / taskResource;

    String logMessage = "GreedyHeadroomCalculator::getAvailableSlots(): "
                        + "totalResources == " + totalResource
                        + "taskResource   == " + taskResource
                        + "availableSlots == " + availableSlots;

    LOG.info(logMessage);

    return availableSlots;
  }
}
