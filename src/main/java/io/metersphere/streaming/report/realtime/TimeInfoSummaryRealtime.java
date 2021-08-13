package io.metersphere.streaming.report.realtime;

import io.metersphere.streaming.commons.constants.ReportKeys;
import io.metersphere.streaming.commons.utils.LogUtil;
import io.metersphere.streaming.report.base.ReportTimeInfo;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@Component("timeInfoSummaryRealtime")
public class TimeInfoSummaryRealtime extends AbstractSummaryRealtime<ReportTimeInfo> {

    @Override
    public String getReportKey() {
        return ReportKeys.TimeInfo.name();
    }

    @Override
    public ReportTimeInfo execute(String reportId, int resourceIndex) {
        AtomicReference<ReportTimeInfo> result = new AtomicReference<>();
        AtomicLong sumDuration = new AtomicLong(0);
        SummaryRealtimeAction action = (resultPart) -> {
            try {
                String reportValue = resultPart.getReportValue();
                ReportTimeInfo reportContent = objectMapper.readValue(reportValue, ReportTimeInfo.class);
                sumDuration.addAndGet(reportContent.getDuration());

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
                long seconds = Duration.between(Instant.ofEpochMilli(reportTimeInfo.getStartTime()), Instant.ofEpochMilli((reportTimeInfo.getEndTime()))).getSeconds();

                reportTimeInfo.setDuration(seconds);
                result.set(reportTimeInfo);

            } catch (Exception e) {
                LogUtil.error("TimeInfoSummaryRealtime: ", e);
            }
        };
        selectRealtimeAndDoSummary(reportId, resourceIndex, getReportKey(), action);

        ReportTimeInfo timeInfo = result.get();
        timeInfo.setRealtimeSumDuration(sumDuration.get());

        return timeInfo;
    }


}
