package io.metersphere.streaming.report.impl;

import io.metersphere.streaming.commons.constants.ReportKeys;
import io.metersphere.streaming.report.base.ChartsData;
import io.metersphere.streaming.report.graph.consumer.DistributedActiveThreadsGraphConsumer;
import io.metersphere.streaming.report.parse.ResultDataParse;
import org.apache.jmeter.report.processor.SampleContext;
import org.apache.jmeter.report.processor.graph.impl.ResponseTimeOverTimeGraphConsumer;

import java.util.List;

public class ResponseTimeChartReport extends AbstractReport {

    @Override
    public String getReportKey() {
        return ReportKeys.ResponseTimeChart.name();
    }

    @Override
    public void execute() {
        SampleContext activeThreadMap = sampleContextMap.get(DistributedActiveThreadsGraphConsumer.class.getSimpleName());
        SampleContext responseTimeMap = sampleContextMap.get(ResponseTimeOverTimeGraphConsumer.class.getSimpleName());

        List<ChartsData> resultList = ResultDataParse.graphMapParsing(activeThreadMap.getData(), "", "yAxis");
        List<ChartsData> responseTimeList = ResultDataParse.graphMapParsing(responseTimeMap.getData(), "", "yAxis2");
        resultList.addAll(responseTimeList);

        saveResult(reportId, resultList);
    }
}
