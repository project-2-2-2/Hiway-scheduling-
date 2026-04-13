package org.wsh.model;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class WorkflowDefinition {
    private final String workflowId;
    private final String displayName;
    private final List<JobDefinition> jobs;
    private final Map<String, JobDefinition> jobsById;
    private final Map<String, Integer> orderByJobId;

    public WorkflowDefinition(String workflowId, String displayName, List<JobDefinition> jobs) {
        this.workflowId = workflowId;
        this.displayName = displayName;
        this.jobs = List.copyOf(jobs);
        this.jobsById = new LinkedHashMap<>();
        this.orderByJobId = new LinkedHashMap<>();
        Set<String> seen = new LinkedHashSet<>();
        for (int i = 0; i < jobs.size(); i++) {
            JobDefinition job = jobs.get(i);
            if (!seen.add(job.id())) {
                throw new IllegalArgumentException("Duplicate job id in workflow " + workflowId + ": " + job.id());
            }
            for (String dependency : job.dependencies()) {
                if (dependency.equals(job.id())) {
                    throw new IllegalArgumentException("Job cannot depend on itself: " + job.id());
                }
                if (!seen.contains(dependency)) {
                    throw new IllegalArgumentException(
                            "Workflow " + workflowId + " must list dependencies before dependents. Missing prior job: "
                                    + dependency + " for " + job.id());
                }
            }
            jobsById.put(job.id(), job);
            orderByJobId.put(job.id(), i);
        }
    }

    public String workflowId() {
        return workflowId;
    }

    public String displayName() {
        return displayName;
    }

    public List<JobDefinition> jobs() {
        return jobs;
    }

    public JobDefinition job(String jobId) {
        JobDefinition job = jobsById.get(jobId);
        if (job == null) {
            throw new IllegalArgumentException("Unknown job id: " + jobId);
        }
        return job;
    }

    public int orderOf(String jobId) {
        Integer index = orderByJobId.get(jobId);
        if (index == null) {
            throw new IllegalArgumentException("Unknown job id: " + jobId);
        }
        return index;
    }

    public List<JobDefinition> successors(String jobId) {
        return jobs.stream()
                .filter(def -> def.dependencies().contains(jobId))
                .toList();
    }

    public List<JobDefinition> trainingRepresentativeJobs() {
        Map<String, JobDefinition> byProfile = new LinkedHashMap<>();
        for (JobDefinition job : jobs) {
            byProfile.putIfAbsent(job.trainingProfileKey(), job);
        }
        return List.copyOf(byProfile.values());
    }
}
