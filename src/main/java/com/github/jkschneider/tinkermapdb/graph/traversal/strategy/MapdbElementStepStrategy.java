package com.github.jkschneider.tinkermapdb.graph.traversal.strategy;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.GraphStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.IdentityStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.StartStep;
import com.tinkerpop.gremlin.process.graph.traversal.strategy.AbstractTraversalStrategy;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.empty.EmptyGraph;

public class MapdbElementStepStrategy extends AbstractTraversalStrategy {
    private static final MapdbElementStepStrategy INSTANCE = new MapdbElementStepStrategy();

    private MapdbElementStepStrategy() {
    }

    @SuppressWarnings("unchecked")
    @Override
    public void apply(final Traversal.Admin<?, ?> traversal, final TraversalEngine engine) {
        if (engine.equals(TraversalEngine.STANDARD))
            return;

        final StartStep<Element> startStep = (StartStep<Element>) traversal.getStartStep();
        if (startStep.startAssignableTo(Vertex.class, Edge.class)) {
            final Element element = ((StartStep<?>) startStep).getStart();
            traversal.removeStep(startStep);
            startStep.getLabel().ifPresent(label -> {
                final Step identityStep = new IdentityStep(traversal);
                identityStep.setLabel(label);
                traversal.addStep(0, identityStep);
            });
            traversal.addStep(0, new GraphStep<>(traversal, EmptyGraph.instance(), element.getClass(), element.id()));
        }
    }

    public static MapdbElementStepStrategy instance() {
        return INSTANCE;
    }
}
