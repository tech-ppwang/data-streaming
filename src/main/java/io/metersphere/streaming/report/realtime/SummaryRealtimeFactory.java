package io.metersphere.streaming.report.realtime;

import io.metersphere.streaming.commons.utils.CommonBeanFactory;
import io.metersphere.streaming.report.summary.Summary;
import org.apache.commons.lang3.StringUtils;

public class SummaryRealtimeFactory {

    public static SummaryRealtime<?> getSummaryExecutor(String reportKey) {
        return CommonBeanFactory.getBean(StringUtils.uncapitalize(reportKey) + "SummaryRealtime", SummaryRealtime.class);
    }
}
