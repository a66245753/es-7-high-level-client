package com.david.dto.request;

import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * @authar David
 * @Date 2025/3/5
 * @description
 */
@Data
public class EsSearchRequest {
    private String index;
    private int pageIndex = 0;
    private int pageSize = 10;
    /**
     * 精确匹配字段值，例如状态、类别、日期、数值等。
     */
    private Map<String, Object> equalsParams;
    /**
     * 全文搜索，例如文章内容、描述等。
     */
    private Map<String, String> likeParams;
    /**
     * 数值范围查询或精确匹配，例如年龄、价格、日期等。
     */
    private List<EsRangeParams> rangeParams;

}
