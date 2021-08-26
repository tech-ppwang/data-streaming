package io.metersphere.streaming.report.realtime;

import com.fasterxml.jackson.core.type.TypeReference;
import io.metersphere.streaming.commons.constants.ReportKeys;
import io.metersphere.streaming.commons.utils.CommonBeanFactory;
import io.metersphere.streaming.commons.utils.LogUtil;
import io.metersphere.streaming.report.base.ReportTimeInfo;
import io.metersphere.streaming.report.base.Statistics;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Component("requestStatisticsSummaryRealtime")
public class RequestStatisticsSummaryRealtime extends AbstractSummaryRealtime<List<Statistics>> {
    private final BigDecimal oneHundred = new BigDecimal(100);

    @Override
    public String getReportKey() {
        return ReportKeys.RequestStatistics.name();
    }

    @Override
    public List<Statistics> execute(String reportId, int resourceIndex) {
        ReportTimeInfo timeInfo = CommonBeanFactory.getBean(TimeInfoSummaryRealtime.class).execute(reportId, resourceIndex);
        Map<Integer, Long> realtimeSumDurations = timeInfo.getRealtimeSumDurations();

        List<Statistics> result = new ArrayList<>();
        AtomicInteger sort = new AtomicInteger(1);
        SummaryRealtimeAction action = (resultPart) -> {
            try {
                String reportValue = resultPart.getReportValue();
                sort.set(resultPart.getSort());
                List<Statistics> reportContent = objectMapper.readValue(reportValue, new TypeReference<List<Statistics>>() {
                });

                reportContent.forEach(statistics -> {
                    statistics.setTransactions(format.format(new BigDecimal(statistics.getTransactions()).multiply(BigDecimal.valueOf(realtimeSumDurations.get(resultPart.getSort())))));
                    statistics.setReceived(format.format(new BigDecimal(statistics.getReceived()).multiply(BigDecimal.valueOf(realtimeSumDurations.get(resultPart.getSort())))));
                    statistics.setSent(format.format(new BigDecimal(statistics.getSent()).multiply(BigDecimal.valueOf(realtimeSumDurations.get(resultPart.getSort())))));
                });

                // 保存顺序
                List<String> orderList = reportContent.stream().map(Statistics::getLabel).collect(Collectors.toList());
                // 第一遍不需要汇总
                if (CollectionUtils.isEmpty(result)) {
                    result.addAll(reportContent);
                    return;
                }
                // 第二遍以后
                result.addAll(reportContent);

                Map<String, List<Statistics>> collect = result.stream().collect(Collectors.groupingBy(Statistics::getLabel));
                List<Statistics> summaryDataList = collect.keySet().stream().map(k -> {

                    List<Statistics> errorsList = collect.get(k);
                    return getStatistics(k, errorsList);
                }).collect(Collectors.toList());
                // 清空
                result.clear();
                // 保留前几次的结果
                result.addAll(summaryDataList);
                // 按照原始顺序重新排序
                result.sort(Comparator.comparingInt(a -> orderList.indexOf(a.getLabel())));
            } catch (Exception e) {
                LogUtil.error("RequestStatisticsSummaryRealtime: ", e);
            }
        };
        selectRealtimeAndDoSummary(reportId, resourceIndex, getReportKey(), action);
        BigDecimal divisor = new BigDecimal(sort.get());
        //

        result.forEach(statistics -> {
            statistics.setError(format.format(new BigDecimal(statistics.getFail()).divide(new BigDecimal(statistics.getSamples()), 4, BigDecimal.ROUND_HALF_UP).multiply(oneHundred)));
            statistics.setAverage(format.format(new BigDecimal(statistics.getAverage()).divide(divisor, 4, BigDecimal.ROUND_HALF_UP)));
            statistics.setMedian(format.format(new BigDecimal(statistics.getMedian()).divide(divisor, 4, BigDecimal.ROUND_HALF_UP)));
            statistics.setTp90(format.format(new BigDecimal(statistics.getTp90()).divide(divisor, 4, BigDecimal.ROUND_HALF_UP)));
            statistics.setTp95(format.format(new BigDecimal(statistics.getTp95()).divide(divisor, 4, BigDecimal.ROUND_HALF_UP)));
            statistics.setTp99(format.format(new BigDecimal(statistics.getTp99()).divide(divisor, 4, BigDecimal.ROUND_HALF_UP)));
            if (timeInfo.getDuration() > 0) {
                statistics.setTransactions(format.format(new BigDecimal(statistics.getTransactions()).divide(BigDecimal.valueOf(timeInfo.getDuration()), 4, BigDecimal.ROUND_HALF_UP)));
                statistics.setReceived(format.format(new BigDecimal(statistics.getReceived()).divide(BigDecimal.valueOf(timeInfo.getDuration()), 4, BigDecimal.ROUND_HALF_UP)));
                statistics.setSent(format.format(new BigDecimal(statistics.getSent()).divide(BigDecimal.valueOf(timeInfo.getDuration()), 4, BigDecimal.ROUND_HALF_UP)));
            }
        });

        // 把 total 放到最后
        List<Statistics> total = result.stream().filter(r -> StringUtils.equalsAnyIgnoreCase(r.getLabel(), "Total")).collect(Collectors.toList());
        result.removeAll(total);
        result.addAll(total);
        return result;
    }

    private Statistics getStatistics(String k, List<Statistics> statisticsList) {
        BigDecimal samples = BigDecimal.ZERO;
        BigDecimal fail = BigDecimal.ZERO;
        BigDecimal error = BigDecimal.ZERO;
        BigDecimal avg = BigDecimal.ZERO;
        BigDecimal min = new BigDecimal(Integer.MAX_VALUE);
        BigDecimal max = new BigDecimal(Integer.MIN_VALUE);
        BigDecimal med = BigDecimal.ZERO;
        BigDecimal tp90 = BigDecimal.ZERO;
        BigDecimal tp95 = BigDecimal.ZERO;
        BigDecimal tp99 = BigDecimal.ZERO;
        BigDecimal trans = BigDecimal.ZERO;
        BigDecimal received = BigDecimal.ZERO;
        BigDecimal sent = BigDecimal.ZERO;
        for (Statistics statistics : statisticsList) {
            samples = samples.add(new BigDecimal(statistics.getSamples()));
            fail = fail.add(new BigDecimal(statistics.getFail()));
            error = error.add(new BigDecimal(statistics.getError()));
            avg = avg.add(new BigDecimal(statistics.getAverage()));
            if (min.compareTo(new BigDecimal(statistics.getMin())) > 0) {
                min = new BigDecimal(statistics.getMin());
            }

            if (max.compareTo(new BigDecimal(statistics.getMax())) < 0) {
                max = new BigDecimal(statistics.getMax());
            }
            med = med.add(new BigDecimal(statistics.getMedian()));
            tp90 = tp90.add(new BigDecimal(statistics.getTp90()));
            tp95 = tp95.add(new BigDecimal(statistics.getTp95()));
            tp99 = tp99.add(new BigDecimal(statistics.getTp99()));
            trans = trans.add(new BigDecimal(statistics.getTransactions()));
            received = received.add(new BigDecimal(statistics.getReceived()));
            sent = sent.add(new BigDecimal(statistics.getSent()));

        }

        Statistics c = new Statistics();
        c.setLabel(k);
        c.setSamples(samples.toString());
        c.setFail(fail.toString());
        c.setError(error.toString());

        c.setAverage(avg.toString());
        c.setMin(min.toString());
        c.setMax(max.toString());
        c.setMedian(med.toString());

        c.setTp90(tp90.toString());
        c.setTp95(tp95.toString());
        c.setTp99(tp99.toString());

        c.setTransactions(trans.toString());
        c.setReceived(received.toString());
        c.setSent(sent.toString());
        return c;
    }

}
