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


import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.tez.runtime.api.InputInitializerContext;

import java.util.Collections;
import java.util.List;

class ScaledHeadroomCalculator extends QueueHeadroomCalculator {

  private static final Log LOG = LogFactory.getLog(ScaledHeadroomCalculator.class);

  private List<Double> thresholds = Lists.newArrayList();

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    thresholds = getThresholds(HiveConf.getVar(conf,
                                               HiveConf.ConfVars.HIVE_AM_SCALED_HEADROOM_CALCULATOR_DISTRIBUTION));
  }

  private List<Double> getThresholds(String distributionString) {
    try {
      List<String> thresholdStrings = Lists.newArrayList(distributionString.split(","));
      List<Double> thresholds = Lists.newArrayList(Iterables.transform(
          thresholdStrings, new Function<String, Double>() {
            @Override
            public Double apply(String input) {
              return Double.parseDouble(input);
            }
          }
      ));
      Collections.sort(thresholds);
      return thresholds;
    }
    catch (Exception exception) {
      LOG.error("Could not extract thresholds from string: " + distributionString
               + ". Switching to defaults: "
               + HiveConf.ConfVars.HIVE_AM_SCALED_HEADROOM_CALCULATOR_DISTRIBUTION.getDefaultValue(), exception) ;
      return getThresholds(HiveConf.ConfVars.HIVE_AM_SCALED_HEADROOM_CALCULATOR_DISTRIBUTION.getDefaultValue());
    }

  }

  @Override
  public int getAvailableSlots(InputInitializerContext context) {

    int totalResourceMB = context.getTotalAvailableResource().getMemory();
    int i = 0;
    while (i<thresholds.size() && thresholds.get(i) < (totalResourceMB / (1024)) ) {
      ++i;
    }

    int usableTotalResourceMB = (int)(totalResourceMB * (1 - i/(thresholds.size()+1.0)));
    int taskResourceMB = context.getVertexTaskResource().getMemory();

    int availableSlots = usableTotalResourceMB/taskResourceMB;

    LOG.info("ScaledHeadroomCalculator::getAvailableSlots(): totalResourceMB == " + totalResourceMB
             + ", thresholds == " + thresholds
             + ", usableTotalResourceMB == " + usableTotalResourceMB
             + ", taskResourceMB == " + taskResourceMB
             + ", availableSlots == " + availableSlots);

    return availableSlots;
  }

}
