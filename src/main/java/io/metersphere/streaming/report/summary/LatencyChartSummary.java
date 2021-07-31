package io.metersphere.streaming.report.summary;

import io.metersphere.streaming.commons.constants.ReportKeys;
import io.metersphere.streaming.report.base.ChartsData;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component("latencyChartSummary")
public class LatencyChartSummary extends AbstractSummary<List<ChartsData>> {

    @Override
    public String getReportKey() {
        return ReportKeys.LatencyChart.name();
    }

    @Override
    public List<ChartsData> execute(String reportId) {
        return handleAvgAction(reportId);
    }


}
