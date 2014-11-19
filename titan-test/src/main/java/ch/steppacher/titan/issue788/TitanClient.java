package ch.steppacher.titan.issue788;


import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class TitanClient {

	private static final Logger	LOG		= LoggerFactory.getLogger(TitanClient.class);

	private final TitanGraph	graph;

	public TitanClient(final TitanGraph titanGraph) {
		this.graph = titanGraph;
	}

	public void deleteAllOutdated(final long cutOffTime) {
		try {
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			LOG.info("Deleting all vertices and edges that have not been touched before " + df.format(new Date(cutOffTime)));

			// Delete edges
			Iterator<Edge> edges = BlueprintsUtil.getEdgesNotModifiedSince(graph, cutOffTime).iterator();
			Set<Object> edgeIds = new HashSet<>();
			while (edges.hasNext()) {
				Edge edge = edges.next();
				edgeIds.add(edge.getId());
				LOG.info("Deleting outdated edge from graph: " + edge + "; updated at " + df.format(edge.getProperty("UPDATED_AT")));
			}
			for (Object edgeId : edgeIds) {
				deleteEdgeById(edgeId);
			}

			// Delete vertices
			Iterator<Vertex> vertices = BlueprintsUtil.getVerticesNotModifiedSince(graph, cutOffTime).iterator();
			Set<String> vertexUUIDs = new HashSet<>();
			while (vertices.hasNext()) {
				Vertex vertex = vertices.next();
				vertexUUIDs.add((String) vertex.getProperty("UUID"));
				LOG.info("Deleting outdated vertex from graph: " + vertex + ", " + vertex.getProperty("label") + ";  updated at " + (vertex.getProperty("UPDATED_AT") != null ? df.format(vertex.getProperty("UPDATED_AT")) : "n/a"));
			}
			for (String uuid : vertexUUIDs) {
				deleteVertexByUUID(UUID.fromString(uuid));
			}

			graph.commit();
		} catch (Exception e) {
			graph.rollback();
			throw new RuntimeException("Failed to delete outdated edge or vertex.", e);
		}
	}

	private void deleteEdgeById(final Object id) {
		Edge poorSoul = graph.getEdge(id);
		if (poorSoul != null) {
			LOG.debug("Deleting edge " + poorSoul);
			poorSoul.remove();
		}
		else {
			LOG.warn("Cannot delete. Edge with ID " + id + " could not be found in graph.");
		}
	}
	
	private void deleteVertexByUUID(final UUID uuid) {
		Vertex poorSoul = loadVertex(uuid);
		if (poorSoul != null) {
			deleteVertex(poorSoul);
		}
		else {
			LOG.warn("Cannot delete. Entry with UUID '" + uuid + "' could not be found in graph.");
		}
	}
	
	private Vertex loadVertex(final UUID entryUUID) {
		return loadVertex(entryUUID.toString(), "UUID");
	}
	
	private Vertex loadVertex(final String id, final String idProperty) {
		Vertex result = null;
		Iterator<Vertex> verteces = graph.getVertices(idProperty, id).iterator();

		if (verteces.hasNext()) {
			result = verteces.next();
			if (verteces.hasNext()) {
				throw new IllegalStateException("Found more than one vertex for " + idProperty + " '" + id + "'! Graph is not consistent.");
			}
		}

		return result;
	}
	
	private void deleteVertex(Vertex poorSoul) {
		Iterable<Edge> edges = poorSoul.getEdges(Direction.BOTH);
		for (Edge edge : edges) {
			LOG.debug("Deleting edge " + edge);
			edge.remove();
		}
		LOG.debug("Deleting vertex " + poorSoul);
		poorSoul.remove();
	}
}
