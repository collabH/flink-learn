package dev.learn.flink.deploy;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.yarn.YarnClientYarnClusterInformationRetriever;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.configuration.YarnDeploymentTarget;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class FlinkApplicationDeployServiceImpl implements FlinkDeployService {


    /**
     * key: job name, value: cluster client
     */
    private final Map<String, ClusterClient<ApplicationId>> jobNameClusterClientMap = Maps.newConcurrentMap();

    /**
     * key: job name, value: job infos
     */
    private final Map<String, JobStatusMessage> jobInfos = Maps.newConcurrentMap();


    /**
     * Deploy application cluster.
     * Submits job to yarn through application mode.
     * One cluster for one application.
     *
     * @param bo 提交任务参数
     * @return the deployed cluster id / application id, see {@link ApplicationId}
     * @throws Exception
     */
    @Override
    public ApplicationId submit(FlinkSubmitBo bo) throws Exception {
        ApplicationConfiguration appConf = new ApplicationConfiguration(bo.getProgramArgs(), bo.getMainClassName());
        ClusterSpecification clusterSpecification = new ClusterSpecification.ClusterSpecificationBuilder().createClusterSpecification();

        // 使用applicationMode提交任务
        ClusterClientProvider<ApplicationId> provider = getClusterDescriptor(bo)
                .deployApplicationCluster(clusterSpecification, appConf);

        // get and set the current cluster client
        ClusterClient<ApplicationId> currentClusterClient = provider.getClusterClient();

        ApplicationId applicationId = currentClusterClient.getClusterId();

        Collection<JobStatusMessage> jobs = currentClusterClient.listJobs().get();

        // checks there are no name conflicts
        for (JobStatusMessage job : jobs) {
            String jobName = job.getJobName();
            if (jobNameClusterClientMap.containsKey(jobName)) {
                // close the application cluster where the new job is deploying on and reconcile failing in
                // submitting the new job
                currentClusterClient.shutDownCluster();
                throw new RuntimeException("Found job name conflict. Please assign a distinct" +
                        " name for every job.");
            }
        }

        // put the current cluster client to the cluster client map
        // and put the job info to the job info map
        for (JobStatusMessage job : jobs) {
            String jobName = job.getJobName();
            jobNameClusterClientMap.putIfAbsent(jobName, currentClusterClient);
            jobInfos.putIfAbsent(jobName, job);
        }

        return applicationId;
    }

    /**
     * Generates a save point file for the job.
     *
     * @param jobName            the distinct job name
     * @param savepointDirectory the directory where to save the savepoint, can be hdfs path
     */
    @Override
    public String triggerSavepoint(String jobName, @Nullable String savepointDirectory) {
        ClusterClient<ApplicationId> clusterClient = getClusterClientByJobName(jobName);
        JobID jobid = getJobIdByName(jobName);
        try {
            return clusterClient.triggerSavepoint(jobid, savepointDirectory).get(5, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * Cancels the job by specifying job name.
     * Keep in mind that this will shut the whole application cluster down.
     *
     * @param jobName
     */
    @Override
    public void cancelJob(String jobName) {
        ClusterClient<ApplicationId> clusterClient = getClusterClientByJobName(jobName);
        JobID jobid = getJobIdByName(jobName);
        clusterClient.cancel(jobid);
        cleanAfterShutDownCluster(clusterClient);
    }

    /**
     * Cancels the job by specifying job name and triggers savepoint operation.
     * Keep in mind that this will shut the whole application cluster down.
     *
     * @param jobName
     * @param savepointDirectory
     * @return
     */
    @Override
    public String cancelJobWithSavepoint(String jobName, @Nullable String savepointDirectory) {
        ClusterClient<ApplicationId> clusterClient = getClusterClientByJobName(jobName);
        JobID jobId = getJobIdByName(jobName);
        String actualSavepointPath;
        try {
            actualSavepointPath = clusterClient.cancelWithSavepoint(jobId, savepointDirectory).get(5, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e.getMessage());
        }
        cleanAfterShutDownCluster(clusterClient);

        return actualSavepointPath;
    }


    /**
     * Stops job by job name and triggers savepoint.
     *
     * @param jobName
     * @param savepointDirectory
     * @return the path where the savepoint is located
     */
    @Override
    public String stopJobWithSavepoint(String jobName, @Nullable String savepointDirectory) {
        ClusterClient<ApplicationId> clusterClient = getClusterClientByJobName(jobName);
        JobID jobId = getJobIdByName(jobName);

        String actualSavepointPath;
        try {
            actualSavepointPath = clusterClient.stopWithSavepoint(jobId, false, savepointDirectory).get(5, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e.getMessage());
        }

        jobNameClusterClientMap.remove(jobName);
        jobInfos.remove(jobName);

        return actualSavepointPath;
    }

    /**
     * Gets the flink configuration of the cluster where the job is deploy on, listed in {@link Properties}.
     *
     * @param jobName
     * @return the {@link Properties} contains {@link Configuration}
     */
    @Override
    public Properties getFlinkConfiguration(String jobName) {
        ClusterClient<ApplicationId> clusterClient = getClusterClientByJobName(jobName);
        Configuration configuration = clusterClient.getFlinkConfiguration();
        Properties props = new Properties();
        configuration.addAllToProperties(props);

        return props;
    }

    /**
     * Gets the flink's own web ui url string of the job.
     *
     * @param jobName
     * @return url of the application cluster web ui
     */
    @Override
    public String getWebInterfaceUrl(String jobName) {
        return getClusterClientByJobName(jobName).getWebInterfaceURL();
    }

    /**
     * Gets {@link JobStatus} of the job.
     *
     * @param jobName
     * @return
     */
    @Override
    public JobStatus getJobStatus(String jobName) {
        ClusterClient<ApplicationId> clusterClient = getClusterClientByJobName(jobName);
        JobID jobId = getJobIdByName(jobName);
        JobStatus jobStatus;
        try {
            jobStatus = clusterClient.getJobStatus(jobId).get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
        return jobStatus;
    }

    /**
     * Lists all the running, completed and canceled jobs on the application cluster.
     *
     * @param clusterClient {@link ClusterClient}
     * @return
     */
    private Collection<JobStatusMessage> listJobsOnCluster(ClusterClient<ApplicationId> clusterClient) {
        Collection<JobStatusMessage> jobs;
        try {
            jobs = clusterClient.listJobs().get();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
        return jobs;
    }


    /**
     * Remove the cluster reference from map.
     * Do before shutting down the cluster necessarily.
     *
     * @param clusterClient
     */
    private void cleanAfterShutDownCluster(ClusterClient<ApplicationId> clusterClient) {
        Collection<JobStatusMessage> jobs;
        try {
            jobs = clusterClient.listJobs().get();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
        for (JobStatusMessage job : jobs) {
            jobNameClusterClientMap.remove(job.getJobName());
            jobInfos.remove(job.getJobName());
        }
    }

    /**
     * Gets the job id according to the distinct job name.
     *
     * @param jobName
     * @return {@link JobID}
     */
    private JobID getJobIdByName(String jobName) {
        Preconditions.checkNotNull(jobInfos, "can not get job id before submitting any job.");
        return jobInfos.get(jobName).getJobId();
    }

    /**
     * Gets the specific cluster client according to the job name.
     *
     * @param jobName the distinct job name
     * @return {@link ClusterClient}
     */
    private ClusterClient<ApplicationId> getClusterClientByJobName(String jobName) {
        Preconditions.checkNotNull(jobName, "job name can not be null.");
        ClusterClient<ApplicationId> clusterClient = jobNameClusterClientMap.get(jobName);
        Preconditions.checkNotNull(clusterClient, "Can not recognize the job name: " + jobName);
        return clusterClient;
    }

    /**
     * 构造Flink Configuration
     *
     * @param submitBo flink submit param
     * @return Configuration
     */
    private Configuration buildConfiguration(FlinkSubmitBo submitBo) {
        String hadoopUserName = submitBo.getHadoopUserName();
        System.getProperties().setProperty("HADOOP_USER_NAME", hadoopUserName);
        String flinkConfDir = submitBo.getFlinkConfDir();
        String userJarPath = submitBo.getUserJarPath();
        String flinkProvidedJars = submitBo.getFlinkProvidedJars();
        String applicationName = submitBo.getApplicationName();
        String flinkDistJars = submitBo.getFlinkDistJars();
        String savepointPath = submitBo.getSavepointPath();
        boolean allowNonRestoredState = submitBo.isAllowNonRestoredState();
        Integer parallelism = submitBo.getParallelism();
        if (StringUtils.isNotEmpty(flinkConfDir)) {
            throw new RuntimeException("flink config dir can not be null.");
        }
        Configuration flinkConf = GlobalConfiguration.loadConfiguration(flinkConfDir);
        // set the path of user jars
        flinkConf.set(PipelineOptions.JARS, Collections.singletonList(userJarPath));
        // set the path of flink provided jars
        flinkConf.set(YarnConfigOptions.PROVIDED_LIB_DIRS, Collections.singletonList(new Path(flinkProvidedJars).toString()));
        // set the path of flink dist jars
        flinkConf.set(YarnConfigOptions.FLINK_DIST_JAR, flinkDistJars);
        // set the deployment target
        flinkConf.set(DeploymentOptions.TARGET, YarnDeploymentTarget.APPLICATION.getName());
        // set the yarn application name
        flinkConf.set(YarnConfigOptions.APPLICATION_NAME, applicationName);
        // set the max parallelism of pipeline
        flinkConf.set(PipelineOptions.MAX_PARALLELISM, parallelism);
        // set the savepoint restore options
        if (StringUtils.isNotEmpty(savepointPath)) {
            SavepointRestoreSettings savepointRestoreSettings = SavepointRestoreSettings.forPath(savepointPath, allowNonRestoredState);
            SavepointRestoreSettings.toConfiguration(savepointRestoreSettings, flinkConf);
        }
        return flinkConf;
    }


    /**
     * @param submitBo 任务提交参数
     * @return {@link YarnClusterDescriptor}
     */
    private YarnClusterDescriptor getClusterDescriptor(FlinkSubmitBo submitBo) {
        Configuration configuration = buildConfiguration(submitBo);
        YarnConfiguration yarnConf = new YarnConfiguration();
        yarnConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(yarnConf);
        yarnClient.start();
        YarnClientYarnClusterInformationRetriever retriever = YarnClientYarnClusterInformationRetriever.create(yarnClient);
        return new YarnClusterDescriptor(configuration, yarnConf, yarnClient, retriever, true);
    }
}
