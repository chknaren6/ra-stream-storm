package com.rastream.dag;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StreamApplication {
    // Name of the application/task/work to be run
    private final String name;

    // All tasks in DAG - V(G)
    // Key is task.getId() [v1_t0]
    private final Map<String,Task>tasks;

    // All edges in DAG - E(G)
    private final List<Edge>edges;

    // NUmber of Tasks per vertex/Node
    private final Map<Integer,List<Task>>vertexTaskMap;

    public StreamApplication(String name){
        this.name=name;
        this.tasks=new HashMap<>();
        this.edges=new ArrayList<>();
        this.vertexTaskMap=new HashMap<>();
    }


    // Add a task to DAG , automatically groups it under its VertexID
    public void addTask(Task task){
        tasks.put(task.getId(),task);
        vertexTaskMap
                .computeIfAbsent(task.getVertexId(),k->new ArrayList<>())
                .add(task);
    }

    // Add a directed edge between 2 tasks
    public void addEdge(Edge edge){
        edges.add(edge);
    }

    // Convenience method - create and add an edge in one call
    public Edge connect(Task source, Task target, double rate) {
        Edge edge = new Edge(source, target);
        edge.setTupleTransmissionRate(rate);
        addEdge(edge);
        return edge;
    }

    public Edge connect(Task source, Task target){
        Edge edge = new Edge(source,target);
        addEdge(edge);
        return edge;
    }

    // Get all tasks belonging to a specific node/vertex
    // partitioning algo uses this to group related tasks to reduce inter process communication
    public List<Task>getTaskForVertex(int vertexId){
        return vertexTaskMap.getOrDefault(vertexId,new ArrayList<>());
    }


    // Get all edges that cross between 2 different tasks
    // Used to compute Wcut(G) - Sum of cut edge weight
    public List<Edge> getEdgesBetween(Task source,Task target){
        List<Edge> result=new ArrayList<>();
        for(Edge e:edges){
            if(e.getSource().getId().equals(source.getId())&&
            e.getTarget().getId().equals(target.getId())){
                result.add(e);
            }
        }
        return result;
    }

    // total weight of all edges in the graph
    // eq 18
    public double getTotalEdgeWeight(){
        double total=0.0;
        for(Edge e:edges){
            total+=e.getTupleTransmissionRate();
        }
        return total;
    }

    public String getName()            { return name; }
    public List<Edge> getEdges()       { return edges; }
    public Map<String, Task> getTasks(){ return tasks; }
    public int getVertexCount()        { return vertexTaskMap.size(); }
    public int getTaskCount()          { return tasks.size(); }


}
