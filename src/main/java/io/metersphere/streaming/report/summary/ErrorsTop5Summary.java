package io.metersphere.streaming.report.summary;

import com.fasterxml.jackson.core.type.TypeReference;
import io.metersphere.streaming.commons.constants.ReportKeys;
import io.metersphere.streaming.commons.utils.LogUtil;
import io.metersphere.streaming.report.base.ErrorsTop5;
import lombok.AllArgsConstructor;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

@Component("errorsTop5Summary")
public class ErrorsTop5Summary extends AbstractSummary<List<ErrorsTop5>> {
    private final BigDecimal oneHundred = new BigDecimal(100);


    @Override
    public String getReportKey() {
        return ReportKeys.ErrorsTop5.name();
    }

    @Override
    public List<ErrorsTop5> execute(String reportId) {
        List<ErrorsTop5> result = new ArrayList<>();
        Map<String, List<ErrorCount>> sampleErrorCounts = new HashMap<>();

        SummaryAction action = (resultPart) -> {
            try {
                String reportValue = resultPart.getReportValue();
                List<ErrorsTop5> reportContent = objectMapper.readValue(reportValue, new TypeReference<List<ErrorsTop5>>() {
                });
                // 第一遍不需要汇总
                if (CollectionUtils.isEmpty(result)) {
                    result.addAll(reportContent);
                    return;
                }
                // 第二遍以后
                result.addAll(reportContent);

                Map<String, List<ErrorsTop5>> collect = result.stream().collect(Collectors.groupingBy(ErrorsTop5::getSample));
                List<ErrorsTop5> summaryDataList = collect.keySet().stream().map(sample -> {
                    sampleErrorCounts.put(sample, new ArrayList<>());
                    List<ErrorCount> errorCounts = sampleErrorCounts.get(sample);

                    List<ErrorsTop5> errorsList = collect.get(sample);
                    BigDecimal samples = errorsList.stream().map(e -> new BigDecimal(e.getSamples())).reduce(BigDecimal::add).get();
                    BigDecimal errorsAllSize = errorsList.stream().map(e -> new BigDecimal(e.getErrorsAllSize())).reduce(BigDecimal::add).get();
                    errorsList.forEach(e -> {
                        errorCounts.add(new ErrorCount(e.getError1(), e.getError1Size()));
                        errorCounts.add(new ErrorCount(e.getError2(), e.getError2Size()));
                        errorCounts.add(new ErrorCount(e.getError3(), e.getError3Size()));
                        errorCounts.add(new ErrorCount(e.getError4(), e.getError4Size()));
                        errorCounts.add(new ErrorCount(e.getError5(), e.getError5Size()));
                    });


                    Map<String, List<ErrorCount>> collect1 = errorCounts.stream()
                            .filter(e -> StringUtils.isNotBlank(e.error))
                            .collect(Collectors.groupingBy(e -> e.error));

                    List<ErrorsTop5Summary.ErrorCount> sorted = collect1
                            .keySet()
                            .stream()
                            .map(ek -> {
                                Long sum = collect1.get(ek).stream().map(a -> Long.parseLong(a.count)).reduce(Long::sum).get();
                                return new ErrorsTop5Summary.ErrorCount(ek, sum.toString());
                            })
                            .sorted(Comparator.comparing(a -> Long.parseLong(a.count), Comparator.reverseOrder()))
                            .collect(Collectors.toList());


                    // 保存新的排序
                    errorCounts.clear();
                    errorCounts.addAll(sorted);

                    ErrorsTop5 c = new ErrorsTop5();
                    c.setSample(sample);
                    c.setSamples(samples.toString());
                    c.setErrorsAllSize(errorsAllSize.toString());
                    if (errorCounts.size() > 0) {
                        c.setError1(errorCounts.get(0).error);
                        c.setError1Size(errorCounts.get(0).count);
                    }
                    if (errorCounts.size() > 1) {
                        c.setError2(errorCounts.get(1).error);
                        c.setError2Size(errorCounts.get(1).count);
                    }

                    if (errorCounts.size() > 2) {
                        c.setError3(errorCounts.get(2).error);
                        c.setError3Size(errorCounts.get(2).count);
                    }

                    if (errorCounts.size() > 3) {
                        c.setError4(errorCounts.get(3).error);
                        c.setError4Size(errorCounts.get(3).count);
                    }

                    if (errorCounts.size() > 4) {
                        c.setError5(errorCounts.get(4).error);
                        c.setError5Size(errorCounts.get(4).count);
                    }

                    return c;
                }).collect(Collectors.toList());
                // 清空
                result.clear();
                // 保留前几次的结果
                result.addAll(summaryDataList);
                // 返回
            } catch (Exception e) {
                LogUtil.error("ErrorsTop5Summary: ", e);
            }
        };
        selectPartAndDoSummary(reportId, getReportKey(), action);
        return result;
    }

    @AllArgsConstructor
    static class ErrorCount {
        private String error;
        private String count;
    }
}
