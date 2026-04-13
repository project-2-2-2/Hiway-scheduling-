package org.wsh.scheduler;

import org.wsh.model.ClusterProfile;
import org.wsh.model.WorkflowDefinition;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public final class PsoScheduler implements Scheduler {
    private final int particles;
    private final int iterations;
    private final long seed;

    public PsoScheduler() {
        this(18, 28, 42L);
    }

    public PsoScheduler(int particles, int iterations, long seed) {
        this.particles = Math.max(6, particles);
        this.iterations = Math.max(4, iterations);
        this.seed = seed;
    }

    @Override
    public List<org.wsh.model.PlanAssignment> buildPlan(
            WorkflowDefinition workflow,
            List<ClusterProfile> clusters,
            TrainingBenchmarks benchmarks) {
        MetaheuristicSchedulingSupport support = new MetaheuristicSchedulingSupport(workflow, clusters, benchmarks);
        Random random = new Random(seed ^ workflow.workflowId().hashCode() ^ clusters.size());
        List<Particle> swarm = new ArrayList<>();
        MetaheuristicSchedulingSupport.Evaluation globalBest = support.evaluate(support.baselineSolution(), name());
        for (int index = 0; index < particles; index++) {
            MetaheuristicSchedulingSupport.Solution solution = index == 0
                    ? support.baselineSolution()
                    : support.randomSolution(random);
            MetaheuristicSchedulingSupport.Evaluation evaluation = support.evaluate(solution, name());
            swarm.add(new Particle(solution, evaluation, support.size()));
            if (evaluation.objective() < globalBest.objective()) {
                globalBest = evaluation;
            }
        }
        for (int iteration = 0; iteration < iterations; iteration++) {
            for (Particle particle : swarm) {
                updateParticle(particle, globalBest.solution(), support, random);
                MetaheuristicSchedulingSupport.Evaluation evaluation = support.evaluate(particle.position, name());
                if (evaluation.objective() < particle.personalBest.objective()) {
                    particle.personalBest = evaluation;
                }
                if (evaluation.objective() < globalBest.objective()) {
                    globalBest = evaluation;
                }
            }
        }
        return globalBest.plan();
    }

    @Override
    public String name() {
        return "PSO";
    }

    private void updateParticle(
            Particle particle,
            MetaheuristicSchedulingSupport.Solution globalBest,
            MetaheuristicSchedulingSupport support,
            Random random) {
        double inertia = 0.62;
        double cognitive = 1.35;
        double social = 1.50;
        for (int i = 0; i < particle.position.priorityBias().length; i++) {
            particle.priorityVelocity[i] = inertia * particle.priorityVelocity[i]
                    + (cognitive * random.nextDouble() * (particle.personalBest.solution().priorityBias()[i] - particle.position.priorityBias()[i]))
                    + (social * random.nextDouble() * (globalBest.priorityBias()[i] - particle.position.priorityBias()[i]));
            particle.clusterVelocity[i] = inertia * particle.clusterVelocity[i]
                    + (cognitive * random.nextDouble() * (particle.personalBest.solution().clusterBias()[i] - particle.position.clusterBias()[i]))
                    + (social * random.nextDouble() * (globalBest.clusterBias()[i] - particle.position.clusterBias()[i]));
            particle.energyVelocity[i] = inertia * particle.energyVelocity[i]
                    + (cognitive * random.nextDouble() * (particle.personalBest.solution().energyBias()[i] - particle.position.energyBias()[i]))
                    + (social * random.nextDouble() * (globalBest.energyBias()[i] - particle.position.energyBias()[i]));

            particle.position.priorityBias()[i] = support.clampPriority(particle.position.priorityBias()[i] + particle.priorityVelocity[i]);
            particle.position.clusterBias()[i] = support.clampUnit(particle.position.clusterBias()[i] + particle.clusterVelocity[i]);
            particle.position.energyBias()[i] = support.clampUnit(particle.position.energyBias()[i] + particle.energyVelocity[i]);
        }
    }

    private static final class Particle {
        private final MetaheuristicSchedulingSupport.Solution position;
        private final double[] priorityVelocity;
        private final double[] clusterVelocity;
        private final double[] energyVelocity;
        private MetaheuristicSchedulingSupport.Evaluation personalBest;

        private Particle(
                MetaheuristicSchedulingSupport.Solution position,
                MetaheuristicSchedulingSupport.Evaluation personalBest,
                int size) {
            this.position = position.copy();
            this.personalBest = personalBest;
            this.priorityVelocity = new double[size];
            this.clusterVelocity = new double[size];
            this.energyVelocity = new double[size];
        }
    }
}
