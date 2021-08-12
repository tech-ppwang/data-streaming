package io.metersphere.streaming.report.realtime;

import io.metersphere.streaming.commons.constants.ReportKeys;
import io.metersphere.streaming.commons.utils.LogUtil;
import io.metersphere.streaming.report.base.TestOverview;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@Component("overviewSummaryRealtime")
public class OverviewSummaryRealtime extends AbstractSummaryRealtime<TestOverview> {
    private final DecimalFormat format4 = new DecimalFormat("0.0000");
    private final DecimalFormat format2 = new DecimalFormat("0.00");

    @Override
    public String getReportKey() {
        return ReportKeys.Overview.name();
    }

    @Override
    public TestOverview execute(String reportId, int resourceIndex) {
        AtomicReference<TestOverview> result = new AtomicReference<>();
        AtomicInteger sort = new AtomicInteger(0);
        AtomicLong transactions = new AtomicLong(0);
        AtomicLong totalDuration = new AtomicLong(0);
        SummaryRealtimeAction action = (resultRealtime) -> {
            try {
                String reportValue = resultRealtime.getReportValue();
                Long duration = resultRealtime.getDuration();
                totalDuration.addAndGet(duration);
                sort.incrementAndGet();

                TestOverview reportContent = objectMapper.readValue(reportValue, TestOverview.class);
                // 第一遍不需要汇总
                if (result.get() == null) {
                    BigDecimal tx = new BigDecimal(reportContent.getAvgTransactions()).multiply(BigDecimal.valueOf(duration));
                    transactions.set(tx.longValue());
                    result.set(reportContent);
                    return;
                }
                //
                // 第二遍以后
                TestOverview testOverview = result.get();

                BigDecimal bigDecimal2 = new BigDecimal(testOverview.getMaxUsers());
                BigDecimal bigDecimal1 = new BigDecimal(reportContent.getMaxUsers());
                testOverview.setMaxUsers(bigDecimal1.max(bigDecimal2).toString());

                BigDecimal current = new BigDecimal(reportContent.getAvgTransactions()).multiply(BigDecimal.valueOf(duration));
                transactions.addAndGet(current.longValue());

                testOverview.setAvgBandwidth(new BigDecimal(testOverview.getAvgBandwidth()).add(new BigDecimal(reportContent.getAvgBandwidth())).toString());
                testOverview.setErrors(new BigDecimal(testOverview.getErrors()).add(new BigDecimal(reportContent.getErrors())).toString());
                testOverview.setResponseTime90(new BigDecimal(testOverview.getResponseTime90()).add(new BigDecimal(reportContent.getResponseTime90())).toString());
                testOverview.setAvgResponseTime(new BigDecimal(testOverview.getAvgResponseTime()).add(new BigDecimal(reportContent.getAvgResponseTime())).toString());

                result.set(testOverview);

            } catch (Exception e) {
                LogUtil.error("OverviewSummaryRealtime:", e);
            }
        };
        selectRealtimeAndDoSummary(reportId, resourceIndex, getReportKey(), action);

        BigDecimal divisor = new BigDecimal(sort.get());
        TestOverview testOverview = result.get();

        testOverview.setAvgTransactions(format2.format(BigDecimal.valueOf(transactions.get()).divide(BigDecimal.valueOf(totalDuration.get()), 4, BigDecimal.ROUND_HALF_UP)));

        testOverview.setErrors(format2.format(new BigDecimal(testOverview.getErrors()).divide(divisor, 4, BigDecimal.ROUND_HALF_UP)));
        testOverview.setAvgBandwidth(format4.format(new BigDecimal(testOverview.getAvgBandwidth()).divide(divisor, 4, BigDecimal.ROUND_HALF_UP)));
        testOverview.setResponseTime90(format4.format(new BigDecimal(testOverview.getResponseTime90()).divide(divisor, 4, BigDecimal.ROUND_HALF_UP)));
        testOverview.setAvgResponseTime(format4.format(new BigDecimal(testOverview.getAvgResponseTime()).divide(divisor, 4, BigDecimal.ROUND_HALF_UP)));

        return testOverview;
    }

}
