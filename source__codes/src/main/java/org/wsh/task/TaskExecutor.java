package org.wsh.task;

import org.wsh.model.NodeProfile;

public interface TaskExecutor {
    TaskResult execute(TaskInputs inputs, NodeProfile nodeProfile) throws Exception;
}
