package exercise_3;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.*;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Exercise_3 {

    private static class VProg extends AbstractFunction3<Long,Tuple2<Integer, List<Object>>,Tuple2<Integer, List<Object>>,Tuple2<Integer, List<Object>>> implements Serializable {
        @Override
        public Tuple2<Integer, List<Object>> apply(Long vertexID, Tuple2<Integer, List<Object>> vertexValue,Tuple2<Integer, List<Object>> message) {
            if (message._1 == Integer.MAX_VALUE) {             // superstep 0
                return vertexValue;
            } else {                                        // superstep > 0
            	if (vertexValue._1 < message._1)
            		return vertexValue;
            	else
            		return message;
            }
        }
    }

    private static class sendMsg extends AbstractFunction1<EdgeTriplet<Tuple2<Integer, List<Object>>,Integer>, Iterator<Tuple2<Object,Tuple2<Integer, List<Object>>>>> implements Serializable {
        @Override
        public Iterator<Tuple2<Object,Tuple2<Integer, List<Object>>>> apply(EdgeTriplet<Tuple2<Integer, List<Object>>,Integer> triplet) {        	
        	Tuple2<Object, Tuple2<Integer, List<Object>>> sourceVertex = triplet.toTuple()._1();
            Tuple2<Object, Tuple2<Integer, List<Object>>> dstVertex = triplet.toTuple()._2();

            if (sourceVertex._2._1 == Integer.MAX_VALUE || sourceVertex._2._1 + triplet.attr >= dstVertex._2._1) {
                // do nothing
                return JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<Object, Tuple2<Integer, List<Object>>>>().iterator()).asScala();
            } else {
                System.out.println("sendMsg: " + sourceVertex + " + " + triplet.attr + " -> " + dstVertex);
                // propagate source vertex value
                List<Object> path = new ArrayList<>();
                path.addAll(sourceVertex._2._2);
                path.add(dstVertex._1);
                Tuple2<Integer, List<Object>> msgValue = new Tuple2(sourceVertex._2._1 + triplet.attr, path);
                Tuple2<Object,Tuple2<Integer, List<Object>>> msg = new Tuple2(triplet.dstId(), msgValue);
                return JavaConverters.asScalaIteratorConverter(Arrays.asList(msg).iterator()).asScala();
            }
        }
    }

    private static class merge extends AbstractFunction2<Tuple2<Integer, List<Object>>,Tuple2<Integer, List<Object>>,Tuple2<Integer, List<Object>>> implements Serializable {
        @Override
        public Tuple2<Integer, List<Object>> apply(Tuple2<Integer, List<Object>> m1, Tuple2<Integer, List<Object>> m2) {
        	if (m1._1 < m2._1)
        		return m1;
        	else
        		return m2;
        }
    }

    public static void shortestPathsExt(JavaSparkContext ctx) {
        Map<Long, String> labels = ImmutableMap.<Long, String>builder()
                .put(1l, "A")
                .put(2l, "B")
                .put(3l, "C")
                .put(4l, "D")
                .put(5l, "E")
                .put(6l, "F")
                .build();

        List<Edge<Integer>> edges = Lists.newArrayList(
                new Edge<Integer>(1l,2l, 4), // A --> B (4)
                new Edge<Integer>(1l,3l, 2), // A --> C (2)
                new Edge<Integer>(2l,3l, 5), // B --> C (5)
                new Edge<Integer>(2l,4l, 10), // B --> D (10)
                new Edge<Integer>(3l,5l, 3), // C --> E (3)
                new Edge<Integer>(5l, 4l, 4), // E --> D (4)
                new Edge<Integer>(4l, 6l, 11) // D --> F (11)
        );

        List<Tuple2<Object,Tuple2<Integer,List<Object>>>> vertices = Lists.newArrayList();
        for (Long vertex_id : labels.keySet()) {
        	Tuple2<Integer,List<Object>> vertexInfo = new Tuple2(Integer.MAX_VALUE, new ArrayList<>());
        	if (labels.get(vertex_id) == "A") {
                List<Object> shortestPath = new ArrayList<>();
                shortestPath.add(vertex_id);
                vertexInfo = new Tuple2(0, shortestPath);
        	}
        	vertices.add(new Tuple2<>(vertex_id, vertexInfo));
        }
        

        JavaRDD<Tuple2<Object,Tuple2<Integer,List<Object>>>> verticesRDD = ctx.parallelize(vertices);
        JavaRDD<Edge<Integer>> edgesRDD = ctx.parallelize(edges);
        Tuple2<Integer,List<Object>> defaultVertexAttr = new Tuple2<>(-1, new ArrayList<>());
        Graph<Tuple2<Integer,List<Object>>,Integer> G = Graph.apply(verticesRDD.rdd(),edgesRDD.rdd(), defaultVertexAttr, StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(),
                scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        GraphOps ops = new GraphOps(G, scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));
        
        Tuple2<Integer, List<Object>> initialMessage = new Tuple2<>(Integer.MAX_VALUE, new ArrayList<>());
        
        ops.pregel(initialMessage,
                Integer.MAX_VALUE,
                EdgeDirection.Out(),
                new VProg(),
                new sendMsg(),
                new merge(),
                ClassTag$.MODULE$.apply(Tuple2.class))
            .vertices()
            .toJavaRDD()
            .foreach(v -> {
                Tuple2<Object,Tuple2<Integer,List<Object>>> vertex = (Tuple2<Object,Tuple2<Integer,List<Object>>>)v;
                System.out.println("Minimum cost to get from "+labels.get(1l)+" to "+labels.get(vertex._1)+" is "+ Lists.transform(vertex._2._2, v1 -> labels.get(v1)) + " with cost " + vertex._2._1);
            });
    }
    
}
