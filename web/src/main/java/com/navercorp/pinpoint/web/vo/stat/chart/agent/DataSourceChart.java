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

package com.navercorp.pinpoint.web.vo.stat.chart.agent;

import com.google.common.collect.ImmutableMap;
import com.navercorp.pinpoint.common.service.ServiceTypeRegistryService;
import com.navercorp.pinpoint.rpc.util.ListUtils;
import com.navercorp.pinpoint.web.util.TimeWindow;
import com.navercorp.pinpoint.web.vo.chart.Chart;
import com.navercorp.pinpoint.web.vo.chart.Point;
import com.navercorp.pinpoint.web.vo.chart.TimeSeriesChartBuilder;
import com.navercorp.pinpoint.web.vo.stat.SampledDataSource;
import com.navercorp.pinpoint.web.vo.stat.chart.StatChart;
import com.navercorp.pinpoint.web.vo.stat.chart.StatChartGroup;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * @author Taejin Koo
 */
public class DataSourceChart implements StatChart {

    private final DataSourceChartGroup dataSourceChartGroup;

    public DataSourceChart(TimeWindow timeWindow, List<SampledDataSource> sampledDataSources, ServiceTypeRegistryService serviceTypeRegistryService) {
        this.dataSourceChartGroup = new DataSourceChartGroup(timeWindow, sampledDataSources, serviceTypeRegistryService);
    }
    @Override
    public StatChartGroup getCharts() {
        return dataSourceChartGroup;
    }

    public int getId() {
        return dataSourceChartGroup.getId();
    }

    public String getServiceType() {
        return dataSourceChartGroup.getServiceTypeName();
    }

    public String getDatabaseName() {
        return dataSourceChartGroup.getDatabaseName();
    }

    public String getJdbcUrl() {
        return dataSourceChartGroup.getJdbcUrl();
    }

    public static class DataSourceChartGroup implements StatChartGroup {

        private final TimeWindow timeWindow;

        private final Map<ChartType, Chart<? extends Point>> dataSourceCharts;

        private final int id;
        private final String serviceTypeName;
        private final String databaseName;
        private final String jdbcUrl;

        public enum DataSourceChartType implements AgentChartType {
            ACTIVE_CONNECTION_SIZE,
            MAX_CONNECTION_SIZE
        }

        public DataSourceChartGroup(TimeWindow timeWindow, List<SampledDataSource> sampledDataSourceList, ServiceTypeRegistryService serviceTypeRegistryService) {
            this.timeWindow = Objects.requireNonNull(timeWindow, "timeWindow must not be null");


            this.dataSourceCharts = newDatasourceChart(sampledDataSourceList);

            if (CollectionUtils.isEmpty(sampledDataSourceList)) {
                this.id = SampledDataSource.UNCOLLECTED_VALUE;
                this.serviceTypeName = SampledDataSource.UNCOLLECTED_STRING;
                this.databaseName = SampledDataSource.UNCOLLECTED_STRING;
                this.jdbcUrl = SampledDataSource.UNCOLLECTED_STRING;
            } else {
                SampledDataSource latestSampledDataSource = ListUtils.getLast(sampledDataSourceList);

                this.id = latestSampledDataSource.getId();
                this.serviceTypeName = serviceTypeRegistryService.findServiceType(latestSampledDataSource.getServiceTypeCode()).getName();
                this.databaseName = latestSampledDataSource.getDatabaseName();
                this.jdbcUrl = latestSampledDataSource.getJdbcUrl();
            }
        }

        private Map<ChartType, Chart<? extends Point>> newDatasourceChart(List<SampledDataSource> sampledDataSourceList) {
            Chart<AgentStatPoint<Integer>> activeConnectionChart = newChart(sampledDataSourceList, SampledDataSource::getActiveConnectionSize);
            Chart<AgentStatPoint<Integer>> maxConnectionChart = newChart(sampledDataSourceList, SampledDataSource::getMaxConnectionSize);

            return ImmutableMap.of(DataSourceChartType.ACTIVE_CONNECTION_SIZE, activeConnectionChart, DataSourceChartType.MAX_CONNECTION_SIZE, maxConnectionChart);
        }

        private Chart<AgentStatPoint<Integer>> newChart(List<SampledDataSource> sampledDataSourceList, Function<SampledDataSource, AgentStatPoint<Integer>> filter) {
            TimeSeriesChartBuilder<AgentStatPoint<Integer>> builder = new TimeSeriesChartBuilder<>(timeWindow, SampledDataSource.UNCOLLECTED_POINT_CREATOR);
            return builder.build(sampledDataSourceList, filter);
        }

        @Override
        public TimeWindow getTimeWindow() {
            return timeWindow;
        }

        @Override
        public Map<ChartType, Chart<? extends Point>> getCharts() {
            return dataSourceCharts;
        }

        public int getId() {
            return id;
        }

        public String getServiceTypeName() {
            return serviceTypeName;
        }

        public String getDatabaseName() {
            return databaseName;
        }

        public String getJdbcUrl() {
            return jdbcUrl;
        }
    }

}
