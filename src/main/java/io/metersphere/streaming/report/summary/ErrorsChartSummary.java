package io.metersphere.streaming.report.summary;

import io.metersphere.streaming.commons.constants.ReportKeys;
import io.metersphere.streaming.report.base.ChartsData;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component("errorsChartSummary")
public class ErrorsChartSummary extends AbstractSummary<List<ChartsData>> {

    @Override
    public String getReportKey() {
        return ReportKeys.ErrorsChart.name();
    }

    @Override
    public List<ChartsData> execute(String reportId) {
        return handleSumAction(reportId);
    }


}
