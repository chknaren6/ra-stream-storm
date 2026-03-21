package com.rastream.model;

import com.rastream.partitioning.Subgraph;

import java.util.ArrayList;
import java.util.List;

public class ComputeNode {
    // Unique node identifier
    private final String id;

    // Available resources -> from linux monitoring
    private double availableCpu;
    private double availableMem;
    private double availableIo;

    // Which subgraphs are currently running on this node
    // A node can run multiple subgraphs
    private final List<Subgraph> assignedSubgraphs;

    // whether this node is active or idle
    private boolean active;

    public ComputeNode(String id, double availableCpu, double availableMem , double availableIo){
        this.id=id;
        this.availableCpu= availableCpu;
        this.availableMem=availableMem;
        this.availableIo=availableIo;
        this.assignedSubgraphs= new ArrayList<>();
        this.active=true;
    }

    // Eq 13
    // Returns true only if CPU and mem are sufficient
    public boolean canAccept(Subgraph s, ResourceModel rm){
        double required=rm.computeSubgraphResourceDemand(s.getTasks());
        return (availableCpu>required)&&(availableMem>required);
    }

    // Eq 15
    // delta=0.50 gamma=0.35
    public double computeSurplus(Subgraph s, ResourceModel rm , double gamma , double delta){
        double required = rm.computeSubgraphResourceDemand(s.getTasks());

        return (gamma*(availableCpu-required) + delta*(availableMem-required)+((1.0-gamma-delta)*(availableIo-required)));
    }

    // Eq 14
    public double computeFitness(Subgraph s, ResourceModel rm, double gamma , double delta){
        if(!canAccept(s,rm)) return 0.0;
        return computeSurplus(s,rm,gamma,delta);
    }

    // Assign subgraph to this node , reduce available resources accordingly
    public void assignSubgraph(Subgraph s, ResourceModel rm ){
        assignedSubgraphs.add(s);
        double used=rm.computeSubgraphResourceDemand(s.getTasks());

        availableCpu = Math.max(0, availableCpu - used);
        availableMem = Math.max(0, availableMem - used);
        availableIo  = Math.max(0, availableIo  - used);
    }

    public void setActive(boolean active){
        this.active=active;
    }

    public String getId()              { return id; }
    public boolean isActive()          { return active; }
    public double getAvailableCpu()    { return availableCpu; }
    public double getAvailableMem()    { return availableMem; }
    public double getAvailableIo()     { return availableIo; }
    public List<Subgraph> getAssignedSubgraphs() {
        return assignedSubgraphs;
    }

    @Override
    public String toString() {
        return id + "[cpu=" + String.format("%.2f", availableCpu)
                + " mem=" + String.format("%.2f", availableMem)
                + " active=" + active + "]";
    }

}
