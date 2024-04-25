package com.htsc.cams.trade.utils;

//import org.springframework.beans.BeansException;
//import org.springframework.beans.factory.annotation.Autowired;
////import org.springframework.beans.factory.annotation.Value;
//import org.springframework.context.ApplicationContext;
//import org.springframework.context.ApplicationContextAware;
////import org.springframework.context.annotation.PropertySource;
//import org.springframework.context.support.ClassPathXmlApplicationContext;
//import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @program: trade
 * @description: spring上下文工具类
 * @author: Chen Chen
 * @create: 2021-06-02 15:57
 **/
//@Component
public class SpringContextUtil //implements ApplicationContextAware
{
    // Spring应用上下文环境
//    private static ApplicationContext applicationContext;
//
//    private static String dubboFile;
//
//    private static ClassPathXmlApplicationContext classPathContext = null;
//
//    private final static Map<String,Object> classPathContextBeanMap = new ConcurrentHashMap<>();
//    /**
//     * 实现ApplicationContextAware接口的回调方法。设置上下文环境
//     *
//     * @param applicationContext
//     */
//    public void setApplicationContext(ApplicationContext applicationContext) {
//        SpringContextUtil.applicationContext = applicationContext;
//    }
//
//
////    @Autowired
////    public SpringContextUtil(@Value("${dubbo.file}")String dubboFiles){
////        dubboFile = dubboFiles;
////        classPathContext = new ClassPathXmlApplicationContext(new String[]{dubboFile});
////    }
//
//    /**
//     * @return ApplicationContext
//     */
//    public static ApplicationContext getApplicationContext() {
//        return applicationContext;
//    }
//
//    /**
//     * 获取对象
//     *
//     * @param name
//     * @return Object
//     * @throws BeansException
//     */
//    public static Object getBean(String name) throws BeansException {
//        return applicationContext.getBean(name);
//    }
//
//    public static ClassPathXmlApplicationContext getDubboContextAndStart(){
//        if(!classPathContext.isRunning()) {
//            classPathContext.start();
//        }
//        return classPathContext;
//    }
//
//    public static Object getBeanFromClassPathContext(String name){
//        return classPathContextBeanMap.computeIfAbsent(name, classPathContext::getBean);
//    }
//
//    public static void closeDubboContext(ClassPathXmlApplicationContext context){
//        if(context.isActive()){
//            context.close();
//        }
//    }
}
