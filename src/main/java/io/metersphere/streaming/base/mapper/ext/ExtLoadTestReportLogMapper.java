package io.metersphere.streaming.base.mapper.ext;

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Update;
import org.springframework.stereotype.Repository;

@Repository
public interface ExtLoadTestReportLogMapper {
    @Update({"UPDATE load_test_report_log ",
            "SET content = concat(content, #{line}) ",
            "WHERE report_id = #{id}"})
    int appendLine(@Param("id") String id, @Param("line") String line);
}
