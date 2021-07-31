package io.metersphere.streaming.report.summary;

import io.metersphere.streaming.commons.constants.ReportKeys;
import io.metersphere.streaming.report.base.ChartsData;
import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component("bytesThroughputChartSummary")
public class BytesThroughputChartSummary extends AbstractSummary<List<ChartsData>> {

    @Override
    public String getReportKey() {
        return ReportKeys.BytesThroughputChart.name();
    }

    @Override
    public List<ChartsData> execute(String reportId) {
        return handleSumAction(reportId);
    }


}
