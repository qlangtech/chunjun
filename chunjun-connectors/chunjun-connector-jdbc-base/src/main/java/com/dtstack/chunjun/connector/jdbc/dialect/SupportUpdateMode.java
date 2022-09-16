package com.dtstack.chunjun.connector.jdbc.dialect;

import com.dtstack.chunjun.sink.WriteMode;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-09-16 14:29
 **/
@Inherited
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface SupportUpdateMode {
    WriteMode[] modes() default {WriteMode.INSERT};
}
