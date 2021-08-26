package io.metersphere.streaming.report.summary;

import io.metersphere.streaming.commons.constants.ReportKeys;
import io.metersphere.streaming.report.base.ChartsData;
import org.springframework.stereotype.Component;

import java.util.List;

@Component("activeThreadsChartSummary")
public class ActiveThreadsChartSummary extends AbstractSummary<List<ChartsData>> {

    @Override
    public String getReportKey() {
        return ReportKeys.ActiveThreadsChart.name();
    }

    @Override
    public List<ChartsData> execute(String reportId) {
        return handleSumAction(reportId);
    }

}
