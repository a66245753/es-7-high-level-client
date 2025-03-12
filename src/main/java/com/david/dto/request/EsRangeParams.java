package com.david.dto.request;

import lombok.Data;

/**
 * @authar David
 * @Date 2025/3/5
 * @description
 */
@Data
public class EsRangeParams {
    private String field;
    private Object gte;
    private Object lte;

}
