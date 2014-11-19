package ch.steppacher.titan.issue788;

import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;

/**
 * The class abstracts certain use cases like "find all paths between two vertices" or load all
 * edges between two adjacent vertices. It is almost Titan agnostic and works, wherever possible, on
 * the <a href="http://www.tinkerpop.com/">TinkerPop</a> API (Gremlin) instead of Titan's extensions
 * thereof. Should we switch the graph DB implementor, then it should be quite easy to adapt this
 * class to the new provider. Given the new provider is also a Tinkerpop implementation.
 * <p>
 * The class is stateless and thus threadsafe.
 * 
 * @author rsteppac
 * @see GraphQueryResult
 */
public class BlueprintsUtil {

	/**
	 * Find all edges with a property {@link PropertyKeys#UPDATED_AT} value that is older than the
	 * given cutoff in Unix time. The lookup is done via an index and does not require traversing
	 * the complete graph. The result is returned as an {@link Iterable} and edges are only loaded
	 * from the graph in calls to the iterator methods hasNext() and next().
	 * 
	 * @param graph
	 *            The graph to search through
	 * @param cutoffTimestamp
	 *            Timestamp to comppare the edge property value against
	 * @return Iterable to traverse all matching edges.
	 */
	public static Iterable<Edge> getEdgesNotModifiedSince(Graph graph, long cutoffTimestamp) {
		GraphQuery query = graph.query();
		Iterable<Edge> edges = query.has("UPDATED_AT", Compare.LESS_THAN, cutoffTimestamp).edges();

		return edges;
	}

	/**
	 * Find all vertices with a property {@link PropertyKeys#UPDATED_AT} value that is older than
	 * the given cutoff in Unix time. The lookup is done via an index and does not require
	 * traversing the complete graph. The result is returned as an {@link Iterable} and vertices are
	 * only loaded from the graph in calls to the iterator methods hasNext() and next().
	 * 
	 * @param graph
	 *            The graph to search through
	 * @param cutoffTimestamp
	 *            Timestamp to comppare the vertex property value against
	 * @return Iterable to traverse all matching vertices.
	 */
	public static Iterable<Vertex> getVerticesNotModifiedSince(Graph graph, long cutoffTimestamp) {
		GraphQuery query = graph.query();
		Iterable<Vertex> vertices = query.has("UPDATED_AT", Compare.LESS_THAN, cutoffTimestamp).vertices();

		return vertices;
	}

}
