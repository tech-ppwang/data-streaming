package io.metersphere.streaming.report.realtime;

import io.metersphere.streaming.commons.utils.CommonBeanFactory;
import org.apache.commons.lang3.StringUtils;

public class SummaryRealtimeFactory {
    public static <T> SummaryRealtime<T> getSummaryExecutor(String reportKey) {
        return (SummaryRealtime<T>) CommonBeanFactory.getBean(StringUtils.uncapitalize(reportKey) + "SummaryRealtime");
    }
}
