package com.lhcz.db2es;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.lhcz.db2es.config.AppConfig;
import com.lhcz.db2es.core.Pipeline;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;

public class Main {
    public static void main(String[] args) throws Exception {
        // 读取 YAML 配置
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        AppConfig config ;
        File file = new File("application.yaml");

// 如果同级目录没有，尝试读取 config/application.yaml (更规范的生产环境写法)
        if (!file.exists()) {
            file = new File("config/application.yaml");
        }

        if (!file.exists()) {
            // 如果还没找到，尝试从 Jar 包内部读取（作为默认保底）
            InputStream is = Main.class.getClassLoader().getResourceAsStream("application.yaml");
            if (is != null) {
                config = mapper.readValue(is, AppConfig.class);
            } else {
                throw new FileNotFoundException("找不到配置文件 application.yaml");
            }
        } else {
            config = mapper.readValue(file, AppConfig.class);
        }
        System.out.println("Starting Db2Es (Java 21) ...");
        Pipeline pipeline = new Pipeline(config);
        pipeline.start();
        pipeline.await();
    }
}