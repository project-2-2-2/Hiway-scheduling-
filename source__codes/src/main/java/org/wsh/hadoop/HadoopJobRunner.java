package org.wsh.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskCompletionEvent;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.wsh.model.NodeProfile;
import org.wsh.task.TaskResult;
import org.wsh.workflow.WorkflowSpec;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class HadoopJobRunner {
    private static final Pattern JOB_ID_PATTERN = Pattern.compile("^job_(\\d+)_(\\d+)$");
    private final HadoopSupport hadoopSupport;

    public HadoopJobRunner(HadoopSupport hadoopSupport) {
        this.hadoopSupport = hadoopSupport;
    }

    public void syncDataRoot(Path localDataRoot) throws Exception {
        hadoopSupport.syncLocalDirectoryToHdfs(localDataRoot, hadoopSupport.normalizedDataRoot());
    }

    public TaskResult executeWorkflowJob(
            WorkflowSpec workflowSpec,
            String jobId,
            NodeProfile nodeProfile,
            Path localRunRoot,
            String hdfsRunRoot) throws Exception {
        HadoopTaskInputs inputs = workflowSpec.resolveHadoopInputs(jobId, hadoopSupport.normalizedDataRoot(), hdfsRunRoot);
        Path localOutputDirectory = localRunRoot.resolve("jobs").resolve(jobId);
        String hdfsOutputPath = workflowSpec.hadoopOutputPath(jobId, inputs.outputDirectory());
        return submit(
                workflowSpec,
                jobId,
                nodeProfile,
                inputs,
                localOutputDirectory,
                hdfsOutputPath,
                hdfsRunRoot,
                nodeProfile.clusterId());
    }

    public TaskResult executeTrainingJob(
            WorkflowSpec workflowSpec,
            String jobId,
            NodeProfile nodeProfile,
            Path localDataRoot) throws Exception {
        HadoopTaskInputs inputs = workflowSpec.resolveHadoopTrainingInputs(jobId, hadoopSupport.normalizedDataRoot());
        Path localOutputDirectory = localDataRoot.resolve("training/generated").resolve(jobId);
        String hdfsOutputPath = workflowSpec.hadoopTrainingOutputPath(hadoopSupport.normalizedDataRoot(), jobId);
        try {
            return submit(
                    workflowSpec,
                    jobId,
                    nodeProfile,
                    inputs,
                    localOutputDirectory,
                    hdfsOutputPath,
                    hadoopSupport.normalizedWorkspaceRoot() + "/training",
                    nodeProfile.clusterId());
        } catch (Exception exception) {
            throw new IOException("Hadoop training job failed for "
                    + workflowSpec.workflowId()
                    + "/"
                    + jobId
                    + " on "
                    + nodeProfile.nodeId()
                    + " [cluster="
                    + nodeProfile.clusterId()
                    + ", memoryMb="
                    + nodeProfile.memoryMb()
                    + ", cpuThreads="
                    + nodeProfile.cpuThreads()
                    + "]", exception);
        }
    }

    private TaskResult submit(
            WorkflowSpec workflowSpec,
            String jobId,
            NodeProfile nodeProfile,
            HadoopTaskInputs inputs,
            Path localOutputDirectory,
            String hdfsOutputPath,
            String hdfsControlRoot,
            String nodeLabelExpression) throws Exception {
        Files.createDirectories(localOutputDirectory);
        hadoopSupport.deleteIfExists(inputs.outputDirectory(), true);
        String controlPath = hdfsControlRoot + "/_control/" + jobId + "-" + UUID.randomUUID() + ".txt";
        hadoopSupport.writeUtf8(controlPath, "run\n");
        try {
            Configuration configuration = hadoopSupport.configuration();
            configuration.setInt("mapreduce.map.memory.mb", Math.max(512, nodeProfile.memoryMb()));
            configuration.setInt("mapreduce.map.cpu.vcores", Math.max(1, nodeProfile.cpuThreads()));
            if (hadoopSupport.enableNodeLabels() && nodeLabelExpression != null && !nodeLabelExpression.isBlank()) {
                configuration.set("mapreduce.map.node-label-expression", nodeLabelExpression);
            }
            HadoopTaskJob.configureTask(
                    configuration,
                    workflowSpec,
                    jobId,
                    nodeProfile,
                    inputs,
                    hdfsOutputPath,
                    workflowSpec.outputDescription(jobId));
            Job job = HadoopTaskJob.createJob(
                    configuration,
                    workflowSpec.workflowId() + "-" + jobId + "-" + nodeProfile.nodeId(),
                    controlPath);
            job.submit();
            waitForCompletion(workflowSpec, jobId, nodeProfile, job);
            Path localOutputPath = workflowSpec.outputPath(jobId, localOutputDirectory);
            hadoopSupport.copyHdfsFileToLocal(hdfsOutputPath, localOutputPath);
            return new TaskResult(localOutputPath, workflowSpec.outputDescription(jobId));
        } finally {
            hadoopSupport.deleteIfExists(controlPath, false);
        }
    }

    private static String buildFailureMessage(
            WorkflowSpec workflowSpec,
            String jobId,
            NodeProfile nodeProfile,
            Job job) {
        StringBuilder message = new StringBuilder("Hadoop job failed for ")
                .append(workflowSpec.workflowId())
                .append("/")
                .append(jobId)
                .append(" on ")
                .append(nodeProfile.nodeId());
        try {
            message.append(" [trackingUrl=").append(job.getTrackingURL()).append("]");
        } catch (Exception ignored) {
        }
        try {
            JobStatus status = job.getStatus();
            if (status != null) {
                message.append(" [state=").append(status.getState()).append("]");
                if (status.getFailureInfo() != null && !status.getFailureInfo().isBlank()) {
                    message.append(System.lineSeparator()).append("Failure info: ").append(status.getFailureInfo());
                }
            }
        } catch (Exception ignored) {
        }
        List<String> diagnostics = new ArrayList<>();
        try {
            for (TaskCompletionEvent event : job.getTaskCompletionEvents(0, 16)) {
                String eventStatus = event.getStatus().name();
                if (!eventStatus.contains("FAIL") && !eventStatus.contains("KILL")) {
                    continue;
                }
                diagnostics.add("Attempt " + event.getTaskAttemptId() + " status=" + eventStatus);
                for (String diagnostic : job.getTaskDiagnostics(event.getTaskAttemptId())) {
                    if (diagnostic != null && !diagnostic.isBlank()) {
                        diagnostics.add(diagnostic.trim());
                    }
                }
                if (diagnostics.size() >= 6) {
                    break;
                }
            }
        } catch (Exception ignored) {
        }
        if (!diagnostics.isEmpty()) {
            message.append(System.lineSeparator()).append("Diagnostics:");
            for (String diagnostic : diagnostics) {
                message.append(System.lineSeparator()).append(" - ").append(diagnostic);
            }
        }
        return message.toString();
    }

    private void waitForCompletion(
            WorkflowSpec workflowSpec,
            String jobId,
            NodeProfile nodeProfile,
            Job job) throws Exception {
        ApplicationId applicationId = applicationId(job.getJobID());
        try (YarnClient yarnClient = YarnClient.createYarnClient()) {
            yarnClient.init(hadoopSupport.configuration());
            yarnClient.start();
            while (true) {
                ApplicationReport report = yarnClient.getApplicationReport(applicationId);
                YarnApplicationState state = report.getYarnApplicationState();
                if (state == YarnApplicationState.FINISHED) {
                    if (report.getFinalApplicationStatus() == FinalApplicationStatus.SUCCEEDED) {
                        return;
                    }
                    throw new IllegalStateException(buildFailureMessage(workflowSpec, jobId, nodeProfile, job, report));
                }
                if (state == YarnApplicationState.FAILED || state == YarnApplicationState.KILLED) {
                    throw new IllegalStateException(buildFailureMessage(workflowSpec, jobId, nodeProfile, job, report));
                }
                Thread.sleep(1000L);
            }
        }
    }

    private static ApplicationId applicationId(JobID jobId) {
        if (jobId == null) {
            throw new IllegalStateException("Submitted Hadoop job did not produce a JobID");
        }
        Matcher matcher = JOB_ID_PATTERN.matcher(jobId.toString());
        if (!matcher.matches()) {
            throw new IllegalStateException("Unsupported Hadoop JobID format: " + jobId);
        }
        return ApplicationId.newInstance(Long.parseLong(matcher.group(1)), Integer.parseInt(matcher.group(2)));
    }

    private static String buildFailureMessage(
            WorkflowSpec workflowSpec,
            String jobId,
            NodeProfile nodeProfile,
            Job job,
            ApplicationReport report) {
        StringBuilder message = new StringBuilder(buildFailureMessage(workflowSpec, jobId, nodeProfile, job));
        if (report != null) {
            message.append(System.lineSeparator())
                    .append("YARN state: ")
                    .append(report.getYarnApplicationState())
                    .append(" / ")
                    .append(report.getFinalApplicationStatus());
            if (report.getTrackingUrl() != null && !report.getTrackingUrl().isBlank()) {
                message.append(System.lineSeparator()).append("Tracking URL: ").append(report.getTrackingUrl());
            }
            if (report.getDiagnostics() != null && !report.getDiagnostics().isBlank()) {
                message.append(System.lineSeparator()).append("Diagnostics: ").append(report.getDiagnostics().trim());
            }
        }
        return message.toString();
    }
}
