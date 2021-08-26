package io.metersphere.streaming.report.summary;

import io.metersphere.streaming.commons.constants.ReportKeys;
import io.metersphere.streaming.commons.utils.LogUtil;
import io.metersphere.streaming.report.base.TestOverview;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.concurrent.atomic.AtomicReference;

@Component("overviewSummary")
public class OverviewSummary extends AbstractSummary<TestOverview> {
    private final DecimalFormat format4 = new DecimalFormat("0.0000");
    private final DecimalFormat format2 = new DecimalFormat("0.00");

    @Override
    public String getReportKey() {
        return ReportKeys.Overview.name();
    }

    @Override
    public TestOverview execute(String reportId) {
        AtomicReference<TestOverview> result = new AtomicReference<>();
        SummaryAction action = (resultPart) -> {
            try {
                String reportValue = resultPart.getReportValue();
                TestOverview reportContent = objectMapper.readValue(reportValue, TestOverview.class);
                // 第一遍不需要汇总
                if (result.get() == null) {
                    result.set(reportContent);
                    return;
                }
                // 第二遍以后
                TestOverview testOverview = result.get();
                testOverview.setMaxUsers(new BigDecimal(testOverview.getMaxUsers()).add(new BigDecimal(reportContent.getMaxUsers())).toString());
                testOverview.setAvgTransactions(new BigDecimal(testOverview.getAvgTransactions()).add(new BigDecimal(reportContent.getAvgTransactions())).toString());

                testOverview.setAvgBandwidth(new BigDecimal(testOverview.getAvgBandwidth()).add(new BigDecimal(reportContent.getAvgBandwidth())).toString());
                testOverview.setErrors(new BigDecimal(testOverview.getErrors()).add(new BigDecimal(reportContent.getErrors())).toString());
                testOverview.setResponseTime90(new BigDecimal(testOverview.getResponseTime90()).add(new BigDecimal(reportContent.getResponseTime90())).toString());
                testOverview.setAvgResponseTime(new BigDecimal(testOverview.getAvgResponseTime()).add(new BigDecimal(reportContent.getAvgResponseTime())).toString());


                result.set(testOverview);

            } catch (Exception e) {
                LogUtil.error("OverviewSummary: ", e);
            }
        };
        int count = selectPartAndDoSummary(reportId, getReportKey(), action);
        BigDecimal divisor = new BigDecimal(count);

        TestOverview testOverview = result.get();


        testOverview.setErrors(format2.format(new BigDecimal(testOverview.getErrors()).divide(divisor, 4, BigDecimal.ROUND_HALF_UP)));
        testOverview.setResponseTime90(format4.format(new BigDecimal(testOverview.getResponseTime90()).divide(divisor, 4, BigDecimal.ROUND_HALF_UP)));
        testOverview.setAvgResponseTime(format4.format(new BigDecimal(testOverview.getAvgResponseTime()).divide(divisor, 4, BigDecimal.ROUND_HALF_UP)));

        return testOverview;
    }


}
