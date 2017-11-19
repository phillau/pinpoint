/*
 * Copyright 2017 NAVER Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.navercorp.pinpoint.web.vo.stat.chart.application;

import com.navercorp.pinpoint.web.util.TimeWindow;
import com.navercorp.pinpoint.web.vo.chart.Chart;
import com.navercorp.pinpoint.web.vo.chart.Point;
import com.navercorp.pinpoint.web.vo.chart.TimeSeriesChartBuilder;
import com.navercorp.pinpoint.web.vo.stat.AggreJoinActiveTraceBo;
import com.navercorp.pinpoint.web.vo.stat.chart.StatChart;
import com.navercorp.pinpoint.web.vo.stat.chart.StatChartGroup;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author minwoo.jung
 */
public class ApplicationActiveTraceChart implements StatChart {

    private final ApplicationActiveTraceChartGroup activeTraceChartGroup;

    public ApplicationActiveTraceChart(TimeWindow timeWindow, List<AggreJoinActiveTraceBo> aggreJoinActiveTraceBos) {
        this.activeTraceChartGroup = new ApplicationActiveTraceChartGroup(timeWindow, aggreJoinActiveTraceBos);
    }

    @Override
    public StatChartGroup getChartGroup() {
        return activeTraceChartGroup;
    }

    public static class ApplicationActiveTraceChartGroup implements StatChartGroup {

        private static final ActiveTracePoint.UncollectedActiveTracePointCreater UNCOLLECTED_ACTIVE_TRACE_POINT = new ActiveTracePoint.UncollectedActiveTracePointCreater();
        private final TimeWindow timeWindow;
        private final Map<ChartType, Chart<? extends Point>> activeTraceChartMap;

        public enum ActiveTraceChartType implements ApplicationChartType {
            ACTIVE_TRACE_COUNT
        }

        public ApplicationActiveTraceChartGroup(TimeWindow timeWindow, List<AggreJoinActiveTraceBo> aggreJoinActiveTraceBoList) {
            this.timeWindow = timeWindow;
            activeTraceChartMap = new HashMap<>();
            List<ActiveTracePoint> activeTraceList = new ArrayList<>(aggreJoinActiveTraceBoList.size());

            for (AggreJoinActiveTraceBo aggreJoinActiveTraceBo : aggreJoinActiveTraceBoList) {
                activeTraceList.add(new ActiveTracePoint(aggreJoinActiveTraceBo.getTimestamp(), aggreJoinActiveTraceBo.getMinTotalCount(), aggreJoinActiveTraceBo.getMinTotalCountAgentId(), aggreJoinActiveTraceBo.getMaxTotalCount(), aggreJoinActiveTraceBo.getMaxTotalCountAgentId(), aggreJoinActiveTraceBo.getTotalCount()));
            }
            TimeSeriesChartBuilder<ActiveTracePoint> chartBuilder = new TimeSeriesChartBuilder<>(this.timeWindow, UNCOLLECTED_ACTIVE_TRACE_POINT);
            activeTraceChartMap.put(ActiveTraceChartType.ACTIVE_TRACE_COUNT, chartBuilder.build(activeTraceList));
        }

        @Override
        public TimeWindow getTimeWindow() {
            return timeWindow;
        }

        @Override
        public Map<ChartType, Chart<? extends Point>> getCharts() {
            return activeTraceChartMap;
        }
    }
}
