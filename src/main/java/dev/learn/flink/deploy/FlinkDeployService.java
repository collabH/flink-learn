package dev.learn.flink.deploy;

import org.apache.flink.api.common.JobStatus;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * @author ouyanghaixiong@forchange.tech
 * @date 2020/8/26
 * @desc
 */
public interface FlinkDeployService {
    ApplicationId submit(FlinkSubmitBo req) throws Exception;

    String triggerSavepoint(String jobName, String savepointDirectory);

    void cancelJob(String jobName);

    String cancelJobWithSavepoint(String jobName, @Nullable String savepointDirectory);

    String stopJobWithSavepoint(String jobName, String savepointDirectory);

    Properties getFlinkConfiguration(String jobName);

    String getWebInterfaceUrl(String jobName);

    JobStatus getJobStatus(String jobName);
}
