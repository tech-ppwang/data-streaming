package io.metersphere.streaming.report.summary;

import io.metersphere.streaming.commons.constants.ReportKeys;
import io.metersphere.streaming.commons.utils.LogUtil;
import io.metersphere.streaming.report.base.ReportTimeInfo;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicReference;

@Component("timeInfoSummary")
public class TimeInfoSummary extends AbstractSummary<ReportTimeInfo> {

    @Override
    public String getReportKey() {
        return ReportKeys.TimeInfo.name();
    }

    @Override
    public ReportTimeInfo execute(String reportId) {
        AtomicReference<ReportTimeInfo> result = new AtomicReference<>();
        SummaryAction action = (resultPart) -> {
            try {
                String reportValue = resultPart.getReportValue();
                ReportTimeInfo reportContent = objectMapper.readValue(reportValue, ReportTimeInfo.class);
                // 第一遍不需要汇总
                if (result.get() == null) {
                    result.set(reportContent);
                    return;
                }
                // 第二遍以后
                ReportTimeInfo reportTimeInfo = result.get();
                if (reportContent.getStartTime() < reportTimeInfo.getStartTime()) {
                    reportTimeInfo.setStartTime(reportContent.getStartTime());
                }

                if (reportContent.getEndTime() > reportTimeInfo.getEndTime()) {
                    reportTimeInfo.setEndTime(reportContent.getEndTime());
                }
                reportTimeInfo.setDuration(reportTimeInfo.getEndTime() - reportTimeInfo.getStartTime());
                result.set(reportTimeInfo);

            } catch (Exception e) {
                LogUtil.error(e);
            }
        };
        selectPartAndDoSummary(reportId, getReportKey(), action);

        return result.get();
    }


}
