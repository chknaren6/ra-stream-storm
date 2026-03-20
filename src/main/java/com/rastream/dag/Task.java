package com.rastream.dag;

public class Task {
    // vertexId is the node/vertex to which the task belongs (i in [v_i,k])
    private final int vertexId;
    // Within the node/vertex what is the id of task (k in [v_i,k])
    private final int taskId;

    // all tasks of the same node/vertex share this
    private final String functionName;

    public Task(int vertexId, int taskId , String functionName){
        this.vertexId=vertexId;
        this.taskId=taskId;
        this.functionName=functionName;
    }
    // A unique string key for this task
    public String getId(){
        return "v"+vertexId+"_t"+taskId;
    }

    public int getVertexId(){ return vertexId;}
    public int getTaskId(){ return taskId;}
    public String getFunctionName(){ return functionName;}

    @Override
    public String toString(){
        return getId()+"("+functionName+")";
    }
}
