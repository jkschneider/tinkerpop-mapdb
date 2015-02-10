import com.github.jkschneider.tinkermapdb.graph.MapdbGraph;
import com.tinkerpop.gremlin.structure.StructureStandardSuite;
import org.junit.runner.RunWith;

@RunWith(StructureStandardSuite.class)
@StructureStandardSuite.GraphProviderClass(provider = MapdbGraphGraphProvider.class, graph = MapdbGraph.class)
public class MapdbGraphTest {
}
