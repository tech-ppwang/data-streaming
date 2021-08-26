package io.metersphere.streaming.report.summary;

import io.metersphere.streaming.commons.utils.CommonBeanFactory;
import org.apache.commons.lang3.StringUtils;

public class SummaryFactory {
    public static Summary<?> getSummaryExecutor(String reportKey) {
        return CommonBeanFactory.getBean(StringUtils.uncapitalize(reportKey) + "Summary", Summary.class);
    }
}
