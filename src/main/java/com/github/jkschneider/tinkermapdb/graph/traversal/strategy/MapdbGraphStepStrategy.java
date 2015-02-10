package com.github.jkschneider.tinkermapdb.graph.traversal.strategy;

import com.github.jkschneider.tinkermapdb.graph.traversal.sideEffect.MapdbGraphStep;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.graph.traversal.step.HasContainerHolder;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.GraphStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.IdentityStep;
import com.tinkerpop.gremlin.process.graph.traversal.strategy.AbstractTraversalStrategy;
import com.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

public class MapdbGraphStepStrategy extends AbstractTraversalStrategy {
    private static final MapdbGraphStepStrategy INSTANCE = new MapdbGraphStepStrategy();

    private MapdbGraphStepStrategy() {
    }

    @SuppressWarnings("unchecked")
    @Override
    public void apply(final Traversal.Admin<?, ?> traversal, final TraversalEngine engine) {
        if (engine.equals(TraversalEngine.COMPUTER))
            return;

        final Step<?, ?> startStep = traversal.getStartStep();
        if (startStep instanceof GraphStep) {
            final GraphStep<?> originalGraphStep = (GraphStep) startStep;
            final MapdbGraphStep<?> tinkerGraphStep = new MapdbGraphStep<>(originalGraphStep);
            TraversalHelper.replaceStep(startStep, (Step) tinkerGraphStep, traversal);

            Step<?, ?> currentStep = tinkerGraphStep.getNextStep();
            while (true) {
                if (currentStep instanceof HasContainerHolder) {
                    tinkerGraphStep.hasContainers.addAll(((HasContainerHolder) currentStep).getHasContainers());
                    if (currentStep.getLabel().isPresent()) {
                        final IdentityStep identityStep = new IdentityStep<>(traversal);
                        identityStep.setLabel(currentStep.getLabel().get());
                        TraversalHelper.insertAfterStep(identityStep, currentStep, traversal);
                    }
                    traversal.removeStep(currentStep);
                } else if (currentStep instanceof IdentityStep) {
                    // do nothing
                } else {
                    break;
                }
                currentStep = currentStep.getNextStep();
            }
        }
    }

    public static MapdbGraphStepStrategy instance() {
        return INSTANCE;
    }
}
