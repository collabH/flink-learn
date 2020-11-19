package dev.learn.flink.deploy;

import lombok.Data;

@Data
public class FlinkSubmitBo {
    private String applicationName;
    private String[] programArgs;
    private String mainClassName;
    private String userJarPath;
    private String savepointPath;
    private boolean allowNonRestoredState;
    private Integer parallelism = 3;
    private String flinkConfDir;
    /**
     * flink运行所需lib环境
     */
    private String flinkProvidedJars;

    /**
     * flink-yarn jar包
     */
    private String flinkDistJars;

    /**
     * hadoop用户名
     */
    private String hadoopUserName;
}