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

import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.core.Order;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.graphdb.relations.RelationIdentifier;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

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

	private static TitanGraph			graph;
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

		graph = TitanFactory.open(CONFIG);
		titanClient = new TitanClient(graph);
		
		createSchema(graph.getManagementSystem());
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		graph.shutdown();
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
		v1 = graph.addVertexWithLabel("ORG");
		v1.setProperty("UPDATED_AT", now);
		v1.setProperty("UUID", UUID.randomUUID());

		// deleteAllOutdatedTest: Not deleted
		v2 = graph.addVertexWithLabel("ORG");
		v2.setProperty("UPDATED_AT", now + 1);
		v2.setProperty("UUID", UUID.randomUUID());

		// deleteAllOutdatedTest: Deleted
		v3 = graph.addVertexWithLabel("ORG");
		v3.setProperty("UPDATED_AT", now - 1);
		v3.setProperty("UUID", UUID.randomUUID());

		// Create edges
		// deleteAllOutdatedTest: Deleted because too old
		v1v2old = graph.addEdge(null, v1, v2, "CONTAINS");
		v1v2old.setProperty("UPDATED_AT", now - 1);

		// deleteAllOutdatedTest: Not deleted
		v2v1new = graph.addEdge(null, v2, v1, "CONTAINS");
		v2v1new.setProperty("UPDATED_AT", now);

		// deleteAllOutdatedTest: Deleted because V3 is deleted, even though timestamp is up-to-date
		v1v3 = graph.addEdge(null, v1, v3, "CONTAINS");
		v1v3.setProperty("UPDATED_AT", now);

		// deleteAllOutdatedTest: Deleted because V3 is deleted and because the timestamp is outdated
		v3v2 = graph.addEdge(null, v3, v2, "CONTAINS");
		v3v2.setProperty("UPDATED_AT", now - 1);

		graph.commit();
	}

	@After
	public void tearDown() throws Exception {
		int edgeCount = 0;
		int vertexCount = 0;
		for (Edge edge : graph.query().edges()) {
			edge.remove();
			edgeCount++;
		}
		for (Vertex vertex : graph.query().vertices()) {
			vertex.remove();
			vertexCount++;
		}
		graph.commit();

		LOG.debug("Number of edges removed: " + edgeCount + "; number of vertics removed: " + vertexCount);
	}

	@Test
	public void deleteAllOutdatedTest() throws IllegalArgumentException, IllegalAccessException {
		int edgeCnt = 0;
		int vertexCnt = 0;

		for (Edge edge : graph.query().edges()) {
			LOG.debug(edge + " updated at " + edge.getProperty("UPDATED_AT"));
			edgeCnt++;
		}
		for (Vertex vertex : graph.query().vertices()) {
			LOG.debug(vertex + " updated at " + vertex.getProperty("UPDATED_AT"));
			vertexCnt++;
		}

		assertEquals("Unexpected number of vertices", vertexCnt, 3);
		assertEquals("Unexpected number of edges", edgeCnt, 4);

		titanClient.deleteAllOutdated(now);

		@SuppressWarnings("unchecked")
		Iterator<Vertex> vertices = graph.query().orderBy("UPDATED_AT", Order.ASC).vertices().iterator();

		// Should return v1
		Vertex result = vertices.next();
		assertEquals("Unexpected vertex returned", v1.getId(), result.getId());
		assertFalse("Unexpected edge returned", v1.getEdges(Direction.OUT).iterator().hasNext());
		assertTrue("No edge returned", v1.getEdges(Direction.IN).iterator().hasNext());

		// Should return v2
		result = vertices.next();
		assertEquals("Unexpected vertex returned", v2.getId(), result.getId());
		assertFalse("Unexpected edge returned", v2.getEdges(Direction.IN).iterator().hasNext());
		assertTrue("No edge returned", v2.getEdges(Direction.OUT).iterator().hasNext());

		// Should not return v3 as it was deleted
		assertFalse(vertices.hasNext());

		// Edges are not transitioned across transaction boundaries and need to be reloaded.
		int edgeCount = 0;
		for (@SuppressWarnings("unused")
		Edge edge : graph.query().edges()) {
			edgeCount++;
		}
		assertEquals("Unexpected number of edges returned.", 1, edgeCount);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void updateEmergencyTest() throws InterruptedException {
		setupHpsAndPatients();
		assertFalse("There must be no pre-existing emergencies.", graph.query().has("label", "EMERGENCY").edges().iterator().hasNext());
		
		Vertex hp = (Vertex)graph.query().has("HP_ID", HP_ID_1).vertices().iterator().next();
		Vertex patient = (Vertex)graph.query().has("PATIENT_ID", PAT_ID_1).vertices().iterator().next();
		Edge emergency = graph.addEdge(null, hp, patient, "EMERGENCY");
		emergency.setProperty("UPDATED_AT", System.currentTimeMillis());
		emergency.setProperty("EDGE_ID", ((RelationIdentifier)emergency.getId()).getRelationId());
		
		graph.commit();

		Iterator<Edge> emergencyEdges = graph.query().has("label", "EMERGENCY").edges().iterator();
		assertTrue(emergencyEdges.hasNext());
		Edge emergencyEdge = emergencyEdges.next();
		assertFalse(emergencyEdges.hasNext());

		Vertex hpVertex = emergencyEdge.getVertex(Direction.OUT);
		Vertex patientVertex = emergencyEdge.getVertex(Direction.IN);
		assertEquals(HP_ID_1, hpVertex.getProperty("HP_ID"));
		assertEquals(PAT_ID_1, patientVertex.getProperty("PATIENT_ID"));

		graph.commit();
		
		Thread.sleep(2);

		Edge emergencyNew = (Edge)graph.query().has("EDGE_ID", emergency.getProperty("EDGE_ID")).edges().iterator().next();
		/////////////////////////////////////////////////////////////////////////////////////////////////////
		// COMMENT OUT THE LINE BELOW (graph.commit()) AND THE TEST "deleteAllOutdatedTest" will go green. //
		/////////////////////////////////////////////////////////////////////////////////////////////////////
		emergencyNew.setProperty("UPDATED_AT", System.currentTimeMillis());
		graph.commit();
		
		assertFalse(emergency == emergencyNew);
	}

	private void setupHpsAndPatients() {
		// Create HPs
		Vertex hp;
		hp = graph.addVertexWithLabel("HP");
		hp.setProperty("UUID", UUID.randomUUID().toString());
		hp.setProperty("HP_ID", HP_ID_1);
		hp = graph.addVertexWithLabel("HP");
		hp.setProperty("UUID", UUID.randomUUID().toString());
		hp.setProperty("HP_ID", HP_ID_2);
		hp = graph.addVertexWithLabel("HP");
		hp.setProperty("UUID", UUID.randomUUID().toString());
		hp.setProperty("HP_ID", HP_ID_3);

		// Create Patients
		Vertex patient;
		patient = graph.addVertexWithLabel("PATIENT");
		patient.setProperty("PATIENT_ID", PAT_ID_1);
		patient = graph.addVertexWithLabel("PATIENT");
		patient.setProperty("PATIENT_ID", PAT_ID_2);
		patient = graph.addVertexWithLabel("PATIENT");
		patient.setProperty("PATIENT_ID", PAT_ID_3);

		graph.commit();
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

	private static void createSchema(TitanManagement tm) {
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
