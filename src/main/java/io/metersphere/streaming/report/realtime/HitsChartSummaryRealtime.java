package io.metersphere.streaming.report.realtime;

import io.metersphere.streaming.commons.constants.ReportKeys;
import io.metersphere.streaming.report.base.ChartsData;
import org.springframework.stereotype.Component;

import java.util.List;

@Component("hitsChartSummaryRealtime")
public class HitsChartSummaryRealtime extends AbstractSummaryRealtime<List<ChartsData>> {

    @Override
    public String getReportKey() {
        return ReportKeys.HitsChart.name();
    }

    @Override
    public List<ChartsData> execute(String reportId, int resourceIndex) {
        return handleMaxAction(reportId, resourceIndex);
    }

}
