package com.david.dto.response;

import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * @authar David
 * @Date 2025/3/5
 * @description
 */
@Data
public class EsSearchResult {
    private int pageIndex;
    private int pageSize;
    private long total;
    private List<Map<String, Object>> list;
}
