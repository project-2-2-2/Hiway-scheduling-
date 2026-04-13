package org.wsh.scheduler;

import org.wsh.model.ClusterProfile;
import org.wsh.model.WorkflowDefinition;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

public final class GeneticAlgorithmScheduler implements Scheduler {
    private final int populationSize;
    private final int generations;
    private final long seed;

    public GeneticAlgorithmScheduler() {
        this(26, 32, 84L);
    }

    public GeneticAlgorithmScheduler(int populationSize, int generations, long seed) {
        this.populationSize = Math.max(8, populationSize);
        this.generations = Math.max(4, generations);
        this.seed = seed;
    }

    @Override
    public List<org.wsh.model.PlanAssignment> buildPlan(
            WorkflowDefinition workflow,
            List<ClusterProfile> clusters,
            TrainingBenchmarks benchmarks) {
        MetaheuristicSchedulingSupport support = new MetaheuristicSchedulingSupport(workflow, clusters, benchmarks);
        Random random = new Random(seed ^ workflow.workflowId().hashCode() ^ (clusters.size() * 17L));
        List<MetaheuristicSchedulingSupport.Evaluation> population = new ArrayList<>();
        population.add(support.evaluate(support.baselineSolution(), name()));
        while (population.size() < populationSize) {
            population.add(support.evaluate(support.randomSolution(random), name()));
        }
        MetaheuristicSchedulingSupport.Evaluation best = population.stream()
                .min(Comparator.comparingDouble(MetaheuristicSchedulingSupport.Evaluation::objective))
                .orElseThrow();
        for (int generation = 0; generation < generations; generation++) {
            population.sort(Comparator.comparingDouble(MetaheuristicSchedulingSupport.Evaluation::objective));
            List<MetaheuristicSchedulingSupport.Evaluation> next = new ArrayList<>();
            int eliteCount = Math.max(2, populationSize / 8);
            for (int i = 0; i < eliteCount && i < population.size(); i++) {
                next.add(population.get(i));
            }
            while (next.size() < populationSize) {
                MetaheuristicSchedulingSupport.Evaluation parentA = tournament(population, random);
                MetaheuristicSchedulingSupport.Evaluation parentB = tournament(population, random);
                MetaheuristicSchedulingSupport.Solution child = crossover(parentA.solution(), parentB.solution(), support, random);
                mutate(child, support, random);
                next.add(support.evaluate(child, name()));
            }
            population = next;
            MetaheuristicSchedulingSupport.Evaluation generationBest = population.stream()
                    .min(Comparator.comparingDouble(MetaheuristicSchedulingSupport.Evaluation::objective))
                    .orElseThrow();
            if (generationBest.objective() < best.objective()) {
                best = generationBest;
            }
        }
        return best.plan();
    }

    @Override
    public String name() {
        return "GA";
    }

    private MetaheuristicSchedulingSupport.Evaluation tournament(
            List<MetaheuristicSchedulingSupport.Evaluation> population,
            Random random) {
        MetaheuristicSchedulingSupport.Evaluation best = null;
        int rounds = Math.min(4, population.size());
        for (int i = 0; i < rounds; i++) {
            MetaheuristicSchedulingSupport.Evaluation candidate = population.get(random.nextInt(population.size()));
            if (best == null || candidate.objective() < best.objective()) {
                best = candidate;
            }
        }
        return best;
    }

    private MetaheuristicSchedulingSupport.Solution crossover(
            MetaheuristicSchedulingSupport.Solution left,
            MetaheuristicSchedulingSupport.Solution right,
            MetaheuristicSchedulingSupport support,
            Random random) {
        double[] priority = new double[left.priorityBias().length];
        double[] cluster = new double[left.clusterBias().length];
        double[] energy = new double[left.energyBias().length];
        for (int i = 0; i < priority.length; i++) {
            double blend = random.nextDouble();
            priority[i] = support.clampPriority((blend * left.priorityBias()[i]) + ((1.0 - blend) * right.priorityBias()[i]));
            cluster[i] = support.clampUnit((blend * left.clusterBias()[i]) + ((1.0 - blend) * right.clusterBias()[i]));
            energy[i] = support.clampUnit((blend * left.energyBias()[i]) + ((1.0 - blend) * right.energyBias()[i]));
        }
        return new MetaheuristicSchedulingSupport.Solution(priority, cluster, energy);
    }

    private void mutate(MetaheuristicSchedulingSupport.Solution child, MetaheuristicSchedulingSupport support, Random random) {
        double mutationRate = 0.18;
        for (int i = 0; i < child.priorityBias().length; i++) {
            if (random.nextDouble() < mutationRate) {
                child.priorityBias()[i] = support.clampPriority(child.priorityBias()[i] + random.nextGaussian() * 0.18);
            }
            if (random.nextDouble() < mutationRate) {
                child.clusterBias()[i] = support.clampUnit(child.clusterBias()[i] + random.nextGaussian() * 0.12);
            }
            if (random.nextDouble() < mutationRate) {
                child.energyBias()[i] = support.clampUnit(child.energyBias()[i] + random.nextGaussian() * 0.12);
            }
        }
    }
}
