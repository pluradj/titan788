package ch.steppacher.titan.issue788;


import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.janusgraph.core.JanusGraph;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

public class TitanClient {

	private static final Logger	LOG		= LoggerFactory.getLogger(TitanClient.class);

	private final JanusGraph	graph;
	private final GraphTraversalSource g;

	public TitanClient(final JanusGraph titanGraph, final GraphTraversalSource gts) {
		this.graph = titanGraph;
		this.g = gts;
	}

	public void deleteAllOutdated(final long cutOffTime) {
		try {
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			LOG.info("Deleting all vertices and edges that have not been touched before " + df.format(new Date(cutOffTime)));

			// Delete edges
			Iterator<Edge> edges = BlueprintsUtil.getEdgesNotModifiedSince(g, cutOffTime).iterator();
			Set<Object> edgeIds = new HashSet<>();
			while (edges.hasNext()) {
				Edge edge = edges.next();
				edgeIds.add(edge.id());
				LOG.info("Deleting outdated edge from graph: " + edge + "; updated at " + df.format(edge.value("UPDATED_AT")));
			}
			for (Object edgeId : edgeIds) {
				deleteEdgeById(edgeId);
			}

			// Delete vertices
			Iterator<Vertex> vertices = BlueprintsUtil.getVerticesNotModifiedSince(g, cutOffTime).iterator();
			Set<String> vertexUUIDs = new HashSet<>();
			while (vertices.hasNext()) {
				Vertex vertex = vertices.next();
				vertexUUIDs.add((String) vertex.value("UUID"));
				LOG.info("Deleting outdated vertex from graph: " + vertex + ", " + vertex.label() + ";  ");
				Long updatedAt = vertex.value("UPDATED_AT");
				LOG.info("updated at " + (updatedAt != null ? df.format(new Date(updatedAt)) : "n/a"));
			}
			for (String uuid : vertexUUIDs) {
				deleteVertexByUUID(UUID.fromString(uuid));
			}

			graph.tx().commit();
		} catch (Exception e) {
			graph.tx().rollback();
			throw new RuntimeException("Failed to delete outdated edge or vertex.", e);
		}
	}

	private void deleteEdgeById(final Object id) {
		Iterator<Edge> t = graph.edges(id);
		if (t.hasNext()) {
			Edge poorSoul = t.next();
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
		Iterator<Vertex> vertices = g.V().has(idProperty, id).toList().iterator();

		if (vertices.hasNext()) {
			result = vertices.next();
			if (vertices.hasNext()) {
				throw new IllegalStateException("Found more than one vertex for " + idProperty + " '" + id + "'! Graph is not consistent.");
			}
		}

		return result;
	}

	private void deleteVertex(Vertex poorSoul) {
		Iterator<Edge> edges = poorSoul.edges(Direction.BOTH);
		while (edges.hasNext()) {
			Edge edge = edges.next();
			LOG.debug("Deleting edge " + edge);
			edge.remove();
		}
		LOG.debug("Deleting vertex " + poorSoul);
		poorSoul.remove();
	}
}
