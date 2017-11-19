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
import com.navercorp.pinpoint.web.vo.stat.AggreJoinDataSourceBo;
import com.navercorp.pinpoint.web.vo.stat.chart.StatChart;
import com.navercorp.pinpoint.web.vo.stat.chart.StatChartGroup;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author minwoo.jung
 */
public class ApplicationDataSourceChart implements StatChart {

    private final ApplicationDataSourceChartGroup applicationDataSourceChartGroup;

    public ApplicationDataSourceChart(TimeWindow timeWindow, String url, String serviceTypeCodeName, List<AggreJoinDataSourceBo> aggreJoinDataSourceBoList) {
        this.applicationDataSourceChartGroup = new ApplicationDataSourceChartGroup(timeWindow, url, serviceTypeCodeName, aggreJoinDataSourceBoList);
    }

    @Override
    public StatChartGroup getChartGroup() {
        return applicationDataSourceChartGroup;
    }

    public String getServiceType() {
        return applicationDataSourceChartGroup.getServiceTypeCodeName();
    }

    public String getJdbcUrl() {
        return applicationDataSourceChartGroup.getJdbcUrl();
    }

    public static class ApplicationDataSourceChartGroup implements StatChartGroup {

        private static final DataSourcePoint.UncollectedDataSourcePointCreater UNCOLLECTED_DATASOURCE_POINT = new DataSourcePoint.UncollectedDataSourcePointCreater();

        private final TimeWindow timeWindow;
        private final String url;
        private final String serviceTypeCodeName;
        private final Map<ChartType, Chart<? extends Point>> dataSourceChartMap;

        public enum DataSourceChartType implements ApplicationChartType {
            ACTIVE_CONNECTION_SIZE
        }

        public ApplicationDataSourceChartGroup(TimeWindow timeWindow, String url, String serviceTypeCodeName, List<AggreJoinDataSourceBo> aggreJoinDataSourceBoList) {
            this.timeWindow = timeWindow;
            this.url = url;
            this.serviceTypeCodeName = serviceTypeCodeName;
            this.dataSourceChartMap = new HashMap<>();
            List<DataSourcePoint> activeConnectionCountList = new ArrayList<>(aggreJoinDataSourceBoList.size());

            for (AggreJoinDataSourceBo aggreJoinDataSourceBo : aggreJoinDataSourceBoList) {
                activeConnectionCountList.add(new DataSourcePoint(aggreJoinDataSourceBo.getTimestamp(), aggreJoinDataSourceBo.getMinActiveConnectionSize(), aggreJoinDataSourceBo.getMinActiveConnectionAgentId(), aggreJoinDataSourceBo.getMaxActiveConnectionSize(), aggreJoinDataSourceBo.getMaxActiveConnectionAgentId(), aggreJoinDataSourceBo.getAvgActiveConnectionSize()));
            }
            TimeSeriesChartBuilder<DataSourcePoint> chartBuilder = new TimeSeriesChartBuilder<>(this.timeWindow, UNCOLLECTED_DATASOURCE_POINT);
            dataSourceChartMap.put(DataSourceChartType.ACTIVE_CONNECTION_SIZE, chartBuilder.build(activeConnectionCountList));
        }

        public String getJdbcUrl() {
            return url;
        }

        public String getServiceTypeCodeName() {
            return serviceTypeCodeName;
        }

        @Override
        public TimeWindow getTimeWindow() {
            return timeWindow;
        }

        @Override
        public Map<ChartType, Chart<? extends Point>> getCharts() {
            return dataSourceChartMap;
        }
    }
}
