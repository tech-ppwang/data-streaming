package io.metersphere.streaming.report.summary;

import io.metersphere.streaming.commons.utils.CommonBeanFactory;
import org.apache.commons.lang3.StringUtils;

public class SummaryFactory {
    public static <T> Summary<T> getSummaryExecutor(String reportKey) {
        return (Summary<T>) CommonBeanFactory.getBean(StringUtils.uncapitalize(reportKey) + "Summary");
    }
}
