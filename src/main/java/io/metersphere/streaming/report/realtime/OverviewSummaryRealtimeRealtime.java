package io.metersphere.streaming.report.realtime;

import io.metersphere.streaming.commons.constants.ReportKeys;
import io.metersphere.streaming.commons.utils.LogUtil;
import io.metersphere.streaming.report.base.TestOverview;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.concurrent.atomic.AtomicReference;

@Component("overviewSummaryRealtime")
public class OverviewSummaryRealtimeRealtime extends AbstractSummaryRealtime<TestOverview> {
    private final DecimalFormat format4 = new DecimalFormat("0.0000");

    @Override
    public String getReportKey() {
        return ReportKeys.Overview.name();
    }

    @Override
    public TestOverview execute(String reportId, int resourceIndex) {
        AtomicReference<TestOverview> result = new AtomicReference<>();
        SummaryRealtimeAction action = (resultRealtime) -> {
            try {
                String reportValue = resultRealtime.getReportValue();
                TestOverview reportContent = objectMapper.readValue(reportValue, TestOverview.class);
                // 第一遍不需要汇总
                if (result.get() == null) {
                    result.set(reportContent);
                    return;
                }
                // 第二遍以后
                TestOverview testOverview = result.get();

                BigDecimal bigDecimal2 = new BigDecimal(testOverview.getMaxUsers());
                BigDecimal bigDecimal1 = new BigDecimal(reportContent.getMaxUsers());
                testOverview.setMaxUsers(bigDecimal1.max(bigDecimal2).toString());

                BigDecimal bigDecimal3 = new BigDecimal(reportContent.getAvgTransactions());
                BigDecimal bigDecimal4 = new BigDecimal(testOverview.getAvgTransactions());
                testOverview.setAvgTransactions(bigDecimal3.max(bigDecimal4).toString());

                BigDecimal bigDecimal5 = new BigDecimal(reportContent.getAvgBandwidth());
                BigDecimal bigDecimal6 = new BigDecimal(testOverview.getAvgBandwidth());
                testOverview.setAvgBandwidth(bigDecimal5.max(bigDecimal6).toString());

                BigDecimal bigDecimal7 = new BigDecimal(reportContent.getErrors());
                BigDecimal bigDecimal8 = new BigDecimal(testOverview.getErrors());
                testOverview.setErrors(bigDecimal7.max(bigDecimal8).toString());

                BigDecimal bigDecimal9 = new BigDecimal(reportContent.getResponseTime90());
                BigDecimal bigDecimal10 = new BigDecimal(testOverview.getResponseTime90());
                testOverview.setResponseTime90(bigDecimal9.max(bigDecimal10).toString());

                BigDecimal bigDecimal11 = new BigDecimal(reportContent.getAvgResponseTime());
                BigDecimal bigDecimal12 = new BigDecimal(testOverview.getAvgResponseTime());
                testOverview.setAvgResponseTime(bigDecimal11.max(bigDecimal12).toString());

                result.set(testOverview);

            } catch (Exception e) {
                LogUtil.error("OverviewSummaryRealtimeRealtime:", e);
            }
        };
        selectRealtimeAndDoSummary(reportId, resourceIndex, getReportKey(), action);
//        BigDecimal divisor = new BigDecimal(1);

        TestOverview testOverview = result.get();


//        testOverview.setErrors(format4.format(new BigDecimal(testOverview.getErrors()).divide(divisor, 4, BigDecimal.ROUND_HALF_UP)));
//        testOverview.setResponseTime90(format4.format(new BigDecimal(testOverview.getResponseTime90()).divide(divisor, 4, BigDecimal.ROUND_HALF_UP)));
//        testOverview.setAvgResponseTime(format4.format(new BigDecimal(testOverview.getAvgResponseTime()).divide(divisor, 4, BigDecimal.ROUND_HALF_UP)));

        return testOverview;
    }

}
