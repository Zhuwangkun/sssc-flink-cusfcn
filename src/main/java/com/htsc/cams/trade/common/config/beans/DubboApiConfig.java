package com.htsc.cams.trade.common.config.beans;

import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * @program: trade
 * @description: com.htsc.cams.trade.common.config.beans
 * @Author: Liu Zhaoyuan
 * @CreateTime: 2022-07-14  13:05
 */
@Data
public class DubboApiConfig {

    private Map<String,String> bondApi;

    private Map<String,String> creditCurveApi;
}