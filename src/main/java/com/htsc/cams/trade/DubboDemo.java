package com.htsc.cams.trade;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import com.alibaba.fastjson.JSONObject;
//import com.htsc.cams.bond.Calculator;
//import com.htsc.cams.trade.utils.SpringContextUtil;
//import com.htsc.htds.dataapi.auth.entity.AuthRequest;
//import org.apache.commons.lang3.RandomUtils;
////import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;


public class DubboDemo {
    Logger logger = LoggerFactory.getLogger(DubboDemo.class);
//    public static void main(String[] args) throws Exception {
//        ClassPathXmlApplicationContext context=new ClassPathXmlApplicationContext("consumer_test.xml");
//        System.out.println(context.toString());
//    }


//    public Map<String,Object> getDubboBondApi(String tradingid, Double yield, String calcType, String calcDate) throws Exception {
//        Map<String, String> bondApi = apiConfig.getBondApi();
//        // 获取spring容器中的接口Bean
//        Calculator calculator = (Calculator) SpringContextUtil.getBeanFromClassPathContext("calculator");
//        // 访问系统鉴权
//        AuthRequest authRequest = new AuthRequest(bondApi.get("htAppId"), bondApi.get("token"));
//        //服务配置中的方法名称
//        String methodId = "xapi";
//        JSONObject params = new JSONObject();
//        //入参传递
//        String traceId = (String)new RandomUtils.nextInt(32);
//        params.put("traceId", traceId);
//        params.put("systemId", bondApi.get("htAppId"));
//        params.put("functionId", "func01");
//        CamsBondCalcVO bondCalcVO = new CamsBondCalcVO();
//        Map<String,Double> bondIdValue = new HashMap<>();
//        bondIdValue.put(tradingid,yield);
//        bondCalcVO.setBondIdValue(bondIdValue);
//        bondCalcVO.setCalcType(calcType);
//        bondCalcVO.setSettlementDate(calcDate);
//        params.put("data", JsonUtil.toJsonString(bondCalcVO));
//        // 调用接口
//        logger.info("调用中台债券计算器接口, traceId="+traceId);
//        String result1 = calculator.executeModel(authRequest, methodId, params.toString());
//        logger.info("调用中台债券计算器接口返回,result="+result1);
//        ExternalResult result = JsonUtil.parseObject(result1, ExternalResult.class);
//        Map<String,Object> dataMap = BaseResult.parse(result, 0);
//        Map<String, Object> bondMap = (Map<String, Object>) dataMap.get(tradingid);
//        if(bondMap != null){
//            if((Boolean)bondMap.get("isValid")){
//                return bondMap;
//            }else{
//                throw new Exception((String)bondMap.get("reason"));
//            }
//        }else{
//            throw new Exception("债券计算器中台返回为空！");
//        }
//    }
}

