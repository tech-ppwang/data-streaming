package io.metersphere.streaming.report.realtime;

import io.metersphere.streaming.commons.constants.ReportKeys;
import io.metersphere.streaming.report.base.ChartsData;
import org.springframework.stereotype.Component;

import java.util.List;

@Component("responseCodeChartSummaryRealtime")
public class ResponseCodeChartSummaryRealtime extends AbstractSummaryRealtime<List<ChartsData>> {

    @Override
    public String getReportKey() {
        return ReportKeys.ResponseCodeChart.name();
    }

    @Override
    public List<ChartsData> execute(String reportId, int resourceIndex) {
        return handleSumAction(reportId, resourceIndex);
    }

}
