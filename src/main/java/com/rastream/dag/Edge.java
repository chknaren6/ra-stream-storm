package com.rastream.dag;

public class Edge {
    private final Task source;  // upstream task (v_i,k)
    private final Task target;  // downstream task (v_j,l)

    // Tr(v_i,k  ,  v_j,l) Avg Tuple Transmission rate
    // Gets updated at runtime by CommunicationModel
    //Tuple rate starts at 0.0 until DataMonitor fills it in
    private double tupleTransmissionRate;

    public Edge(Task source, Task target){
        this.source=source;
        this.target=target;
        this.tupleTransmissionRate=0.0;
    }
    // called by CommunicationModel
    public void setTupleTransmissionRate(double rate){
        this.tupleTransmissionRate=rate;
    }

    public double getTupleTransmissionRate(){
        return tupleTransmissionRate;
    }

    public Task getSource(){return source;}
    public Task getTarget(){return target;}

    @Override
    public String toString(){
        return source.getId()+"->"+target.getId()+"[Tr="+String.format("%.2f",tupleTransmissionRate)+"]";
    }
}
