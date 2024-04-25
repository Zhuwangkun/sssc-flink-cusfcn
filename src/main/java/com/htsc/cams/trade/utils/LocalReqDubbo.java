package com.htsc.cams.trade.utils;


//import com.alibaba.dubbo.config.ApplicationConfig;
//import com.alibaba.dubbo.config.ReferenceConfig;
//import com.alibaba.fastjson.JSON;
////import com.baj.product.client.domain.product.ItemSkuQuery;
//import java.lang.reflect.InvocationTargetException;
//import java.lang.reflect.Method;
//import java.util.ArrayList;


public class LocalReqDubbo {

//    //dubbo服务名 替换自己的
//    static String serviceInterface = "com.htsc.cams.bond.Calculator";
//    //要调用的接口名 替换自己的
//    static String methodName = "xapi";
//    //替换成自己dubbo服务ip
//    static String dubboURl = "168.15.224.20:2181,168.15.224.63:2181,168.8.189.50:2181,168.8.189.51:2181,168.19.225.13:2181";
//
//    public static void main(String[] args) {
//
//        ReferenceConfig reference = new ReferenceConfig();
//
//        // 查询参数
//        try {
//            Class<?> forName = Class.forName(serviceInterface);
//            // 1. 获取dubbo服务链接
//            Object object = init(reference, forName);
//            // 2. 校验获取方法
//            Method method = getMethod(forName);
//
//            // 3. 设置查询参数,接口中多个参数，使用数组形式赋值。
//            Object[] objectArgs = new Object[1];
////            // 构建请求参数，请根据自己的实际情况修改
////            ItemSkuQuery query = new ItemSkuQuery();
////            query.setCurPage(1);
////            query.setPageSize(1000);
////            query.setState(1);
////            query.setShopCode("1075");
//            objectArgs[0] = "1";
//
//
//            // 4. 指定方法，参数 调用dubbo接口方法
//            Object invokeResult = method.invoke(object, objectArgs[0]);
//            // 5. 打印参数调用结果
//            System.out.println("invokeResult = [" + JSON.toJSONString(invokeResult) + "]");
//
//        } catch (ClassNotFoundException e) {
//            e.printStackTrace();
//        } catch (IllegalAccessException e) {
//            e.printStackTrace();
//        } catch (InvocationTargetException e) {
//            e.printStackTrace();
//        }
//
//    }
//
//    /**
//     * 获取需要调用的方法
//     *
//     * @param forName
//     * @return
//     */
//    private static Method getMethod(Class<?> forName) {
//        Method[] methods = forName.getMethods();
//        for (Method m : methods) {
//            if (methodName.equals(m.getName())) {
//                return m;
//            }
//        }
//
//        return null;
//    }
//
//    private static Object init(ReferenceConfig reference, Class<?> forName) {
//        reference.setApplication(new ApplicationConfig("cu-test"));
//        reference.setInterface(forName);
//        reference.setUrl(dubboURl);
//
//        //超时时间
//        reference.setTimeout(30000);
//        //服务版本号
//        reference.setVersion("1.0.2");
//        Object o = reference.get();
//        return o;
//    }

}