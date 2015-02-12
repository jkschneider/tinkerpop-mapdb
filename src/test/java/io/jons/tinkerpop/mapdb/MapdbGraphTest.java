package io.jons.tinkerpop.mapdb;

import com.tinkerpop.gremlin.structure.StructureStandardSuite;
import org.junit.runner.RunWith;

@RunWith(StructureStandardSuite.class)
@StructureStandardSuite.GraphProviderClass(provider = MapdbGraphGraphProvider.class, graph = MapdbGraph.class)
public class MapdbGraphTest {
}
