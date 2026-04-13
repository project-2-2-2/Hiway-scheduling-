package org.wsh.task;

import java.nio.file.Path;

public record TaskResult(Path outputPath, String description) {
}
