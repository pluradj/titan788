package ch.steppacher.titan.issue788;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Iterator;
import java.util.UUID;

import org.apache.commons.configuration.BaseConfiguration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.janusgraph.core.Cardinality;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.relations.RelationIdentifier;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

/**
 * TODO: Add "tests for failure".
 *
 * @author rsteppac
 */
public class TitanClientTest {

	private static final Logger			LOG	= LoggerFactory.getLogger(TitanClientTest.class);

	private static BaseConfiguration	CONFIG;
	private static String				HP_ID_1		= "100";
	private static String				HP_ID_2		= "101";
	private static String				HP_ID_3		= "102";
	private static String				PAT_ID_1	= "200";
	private static String				PAT_ID_2	= "201";
	private static String				PAT_ID_3	= "202";

	private static JanusGraph			graph;
	private static GraphTraversalSource	g;
	private static TitanClient			titanClient;

	private long						now;
	private Vertex						v1, v2, v3;
	private Edge						v1v2old, v2v1new, v1v3, v3v2;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		CONFIG = new BaseConfiguration();
		CONFIG.addProperty("storage.backend", "berkeleyje");
		CONFIG.addProperty("storage.transactions", "true");
		CONFIG.addProperty("storage.directory", System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") + "titan-schema-test");
		CONFIG.addProperty("schema.default", "none");
		CONFIG.addProperty("index.locallucene.backend", "lucene");
		CONFIG.addProperty("index.locallucene.directory", System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") + "searchindex-test");

		graph = JanusGraphFactory.open(CONFIG);
		g = graph.traversal();
		titanClient = new TitanClient(graph, g);

		createSchema(graph.openManagement());
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		graph.close();
		try {
			Path storageDir = Paths.get(System.getProperty("java.io.tmpdir"), "titan-schema-test");
			Files.walkFileTree(storageDir, new DeletingVisitor());
		} catch (Exception e) {
			// NOOP
		}
		try {
			Path indexDir = Paths.get(System.getProperty("java.io.tmpdir"), "searchindex-test");
			Files.walkFileTree(indexDir, new DeletingVisitor());
		} catch (Exception e) {
			// NOOP
		}
	}

	@Before
	public void setUp() throws Exception {
		now = System.currentTimeMillis();
		// Create vertices
		// deleteAllOutdatedTest: Not deleted
		v1 = graph.addVertex("ORG");
		v1.property("UPDATED_AT", now);
		v1.property("UUID", UUID.randomUUID());

		// deleteAllOutdatedTest: Not deleted
		v2 = graph.addVertex("ORG");
		v2.property("UPDATED_AT", now + 1);
		v2.property("UUID", UUID.randomUUID());

		// deleteAllOutdatedTest: Deleted
		v3 = graph.addVertex("ORG");
		v3.property("UPDATED_AT", now - 1);
		v3.property("UUID", UUID.randomUUID());

		// Create edges
		// deleteAllOutdatedTest: Deleted because too old
		v1v2old = v1.addEdge("CONTAINS", v2);
		v1v2old.property("UPDATED_AT", now - 1);

		// deleteAllOutdatedTest: Not deleted
		v2v1new = v2.addEdge("CONTAINS", v1);
		v2v1new.property("UPDATED_AT", now);

		// deleteAllOutdatedTest: Deleted because V3 is deleted, even though timestamp is up-to-date
		v1v3 = v1.addEdge("CONTAINS", v3);
		v1v3.property("UPDATED_AT", now);

		// deleteAllOutdatedTest: Deleted because V3 is deleted and because the timestamp is outdated
		v3v2 = v3.addEdge("CONTAINS", v2);
		v3v2.property("UPDATED_AT", now - 1);

		graph.tx().commit();
	}

	@After
	public void tearDown() throws Exception {
		int edgeCount = 0;
		int vertexCount = 0;
		for (Edge edge : g.E().toList()) {
			edge.remove();
			edgeCount++;
		}
		for (Vertex vertex : g.V().toList()) {
			vertex.remove();
			vertexCount++;
		}
		graph.tx().commit();

		LOG.debug("Number of edges removed: " + edgeCount + "; number of vertics removed: " + vertexCount);
	}

	@Test
	public void deleteAllOutdatedTest() throws IllegalArgumentException, IllegalAccessException {
		int edgeCnt = 0;
		int vertexCnt = 0;

		for (Edge edge : g.E().toList()) {
			LOG.debug(edge + " updated at " + edge.values("UPDATED_AT").next());
			edgeCnt++;
		}
		for (Vertex vertex : g.V().toList()) {
			LOG.debug(vertex + " updated at " + vertex.values("UPDATED_AT").next());
			vertexCnt++;
		}

		assertEquals("Unexpected number of vertices", vertexCnt, 3);
		assertEquals("Unexpected number of edges", edgeCnt, 4);

		titanClient.deleteAllOutdated(now);

		@SuppressWarnings("unchecked")
		Iterator<Vertex> vertices = g.V().order().by("UPDATED_AT", Order.incr).toList().iterator();

		// Should return v1
		Vertex result = vertices.next();
		assertEquals("Unexpected vertex returned", v1.id(), result.id());
		assertFalse("Unexpected edge returned", v1.edges(Direction.OUT).hasNext());
		assertTrue("No edge returned", v1.edges(Direction.IN).hasNext());

		// Should return v2
		result = vertices.next();
		assertEquals("Unexpected vertex returned", v2.id(), result.id());
		assertFalse("Unexpected edge returned", v2.edges(Direction.IN).hasNext());
		assertTrue("No edge returned", v2.edges(Direction.OUT).hasNext());

		// Should not return v3 as it was deleted
		assertFalse(vertices.hasNext());

		// Edges are not transitioned across transaction boundaries and need to be reloaded.
		int edgeCount = 0;
		for (@SuppressWarnings("unused")
		Edge edge : g.E().toList()) {
			edgeCount++;
		}
		assertEquals("Unexpected number of edges returned.", 1, edgeCount);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void updateEmergencyTest() throws InterruptedException {
		setupHpsAndPatients();
		assertFalse("There must be no pre-existing emergencies.", g.E().hasLabel("EMERGENCY").hasNext());

		Vertex hp = (Vertex)g.V().has("HP_ID", HP_ID_1).next();
		Vertex patient = (Vertex)g.V().has("PATIENT_ID", PAT_ID_1).next();
		Edge emergency = hp.addEdge("EMERGENCY", patient);
		emergency.property("UPDATED_AT", System.currentTimeMillis());
		emergency.property("EDGE_ID", ((RelationIdentifier)emergency.id()).getRelationId());

		graph.tx().commit();

		Iterator<Edge> emergencyEdges = g.E().hasLabel("EMERGENCY").toList().iterator();
		assertTrue(emergencyEdges.hasNext());
		Edge emergencyEdge = emergencyEdges.next();
		assertFalse(emergencyEdges.hasNext());

		Vertex hpVertex = emergencyEdge.outVertex();
		Vertex patientVertex = emergencyEdge.inVertex();
		assertEquals(HP_ID_1, hpVertex.value("HP_ID"));
		assertEquals(PAT_ID_1, patientVertex.value("PATIENT_ID"));

		graph.tx().commit();

		Thread.sleep(2);

		Edge emergencyNew = (Edge)g.E().has("EDGE_ID", (String)emergency.value("EDGE_ID")).next();
		/////////////////////////////////////////////////////////////////////////////////////////////////////
		// COMMENT OUT THE LINE BELOW (graph.tx().commit()) AND THE TEST "deleteAllOutdatedTest" will go green. //
		/////////////////////////////////////////////////////////////////////////////////////////////////////
		emergencyNew.property("UPDATED_AT", System.currentTimeMillis());
		graph.tx().commit();

		assertFalse(emergency == emergencyNew);
	}

	private void setupHpsAndPatients() {
		// Create HPs
		Vertex hp;
		hp = graph.addVertex("HP");
		hp.property("UUID", UUID.randomUUID().toString());
		hp.property("HP_ID", HP_ID_1);
		hp = graph.addVertex("HP");
		hp.property("UUID", UUID.randomUUID().toString());
		hp.property("HP_ID", HP_ID_2);
		hp = graph.addVertex("HP");
		hp.property("UUID", UUID.randomUUID().toString());
		hp.property("HP_ID", HP_ID_3);

		// Create Patients
		Vertex patient;
		patient = graph.addVertex("PATIENT");
		patient.property("PATIENT_ID", PAT_ID_1);
		patient = graph.addVertex("PATIENT");
		patient.property("PATIENT_ID", PAT_ID_2);
		patient = graph.addVertex("PATIENT");
		patient.property("PATIENT_ID", PAT_ID_3);

		graph.tx().commit();
	}

	private static class DeletingVisitor extends SimpleFileVisitor<Path> {
		@Override
		public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
			Files.delete(file);
			return FileVisitResult.CONTINUE;
		}

		@Override
		public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
			Files.delete(dir);
			return FileVisitResult.CONTINUE;
		}
	}

	private static void createSchema(JanusGraphManagement tm) {
		tm.makeVertexLabel("ORG").make();
		tm.makeVertexLabel("HP").make();
		tm.makeVertexLabel("PATIENT").make();

		tm.makeEdgeLabel("CONTAINS").multiplicity(Multiplicity.SIMPLE).make();
		tm.makeEdgeLabel("EMERGENCY").multiplicity(Multiplicity.MULTI).make();

		PropertyKey propHpId = tm.makePropertyKey("HP_ID").cardinality(Cardinality.SINGLE).dataType(String.class).make();
		PropertyKey propPatientId = tm.makePropertyKey("PATIENT_ID").cardinality(Cardinality.SINGLE).dataType(String.class).make();
		PropertyKey propUUID = tm.makePropertyKey("UUID").cardinality(Cardinality.SINGLE).dataType(String.class).make();
		PropertyKey propCreatedAt = tm.makePropertyKey("CREATED_AT").cardinality(Cardinality.SINGLE).dataType(Long.class).make();
		PropertyKey propUpdatedAt = tm.makePropertyKey("UPDATED_AT").cardinality(Cardinality.SINGLE).dataType(Long.class).make();
		tm.makePropertyKey("EDGE_ID").dataType(String.class).make();

		tm.buildIndex("HP_ID_IDX", Vertex.class).addKey(propHpId).unique().buildCompositeIndex();
		tm.buildIndex("PATIENT_ID_IDX", Vertex.class).addKey(propPatientId).unique().buildCompositeIndex();
		tm.buildIndex("UUID_IDX", Vertex.class).addKey(propUUID).unique().buildCompositeIndex();
		tm.buildIndex("UPDATEDATIDXEDGE", Edge.class).addKey(propUpdatedAt).buildCompositeIndex();
		tm.buildIndex("UPDATEDATIDXVERTEX", Vertex.class).addKey(propUpdatedAt).buildCompositeIndex();
		tm.buildIndex("UPDATEDATIDXMIXEDEDGE", Edge.class).addKey(propUpdatedAt).buildMixedIndex("locallucene");
		tm.buildIndex("UPDATEDATIDXMIXEDVERTEX", Vertex.class).addKey(propUpdatedAt).buildMixedIndex("locallucene");
		tm.buildIndex("CREATEDATIDXEDGE", Edge.class).addKey(propCreatedAt).buildCompositeIndex();
		tm.buildIndex("CREATEDATIDXVERTEX", Vertex.class).addKey(propCreatedAt).buildCompositeIndex();
		tm.buildIndex("CREATEDATIDXMIXEDEDGE", Edge.class).addKey(propCreatedAt).buildMixedIndex("locallucene");
		tm.buildIndex("CREATEDATIDXMIXEDVERTEX", Vertex.class).addKey(propCreatedAt).buildMixedIndex("locallucene");

		tm.commit();
	}

}
