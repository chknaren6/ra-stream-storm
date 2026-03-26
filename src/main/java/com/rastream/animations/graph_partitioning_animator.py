"""
Graph Partitioning Visualizer with Animation - FIXED VERSION
Based on Ra-Stream: Distributed Stream Processing with Resource-Aware Scheduling

This module provides interactive visualization for graph partitioning,
resource scaling, and task scheduling algorithms.
"""

import numpy as np
import networkx as nx
import matplotlib
matplotlib.use('TkAgg')  # Use TkAgg backend for better interactivity
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.animation import FuncAnimation
from matplotlib.widgets import Slider, Button
import seaborn as sns
from typing import Dict, List, Tuple, Set
import random
from dataclasses import dataclass
from enum import Enum
import warnings

# Suppress matplotlib warnings
warnings.filterwarnings('ignore', category=UserWarning)

# ============================================================================
# DATA STRUCTURES
# ============================================================================

class NodeState(Enum):
    """Enum for node states"""
    UNASSIGNED = 0
    ASSIGNED = 1
    UNDERLOAD = 2
    OVERLOAD = 3
    IDLE = 4

@dataclass
class TaskMetrics:
    """Task metrics container"""
    cpu_requirement: float
    memory_requirement: float
    io_requirement: float
    tuple_rate: float = 1.0

@dataclass
class SubgraphMetrics:
    """Subgraph metrics"""
    cpu_total: float
    memory_total: float
    io_total: float
    internal_edges: float
    cut_edges: float
    task_count: int

@dataclass
class ComputeNodeMetrics:
    """Compute node metrics"""
    cpu_available: float
    memory_available: float
    io_available: float
    cpu_used: float
    memory_used: float
    io_used: float
    subgraph_count: int

# ============================================================================
# GRAPH PARTITIONING ALGORITHM
# ============================================================================

class SimulatedAnnealingPartitioner:
    """
    Simulated Annealing based Graph Partitioner
    Implements Algorithm 1 from Ra-Stream paper
    """
    
    def __init__(self, G: nx.DiGraph, k: int, epsilon: float = 0.7, max_iterations: int = 100):
        """
        Initialize the partitioner
        
        Args:
            G: NetworkX directed graph
            k: Number of partitions/subgraphs
            epsilon: Weight factor for objective function (0-1)
            max_iterations: Maximum iterations for simulated annealing
        """
        self.G = G
        self.k = k
        self.epsilon = epsilon
        self.max_iterations = max_iterations
        self.current_partition = None
        self.best_partition = None
        self.best_fitness = float('inf')
        self.history = []
        
    def initialize_partition(self) -> Dict[int, int]:
        """Initialize random partition"""
        partition = {}
        nodes = list(self.G.nodes())
        for node in nodes:
            partition[node] = random.randint(0, self.k - 1)
        return partition
    
    def calculate_internal_edges(self, partition: Dict[int, int]) -> Dict[int, float]:
        """Calculate internal edges for each subgraph"""
        internal_edges = {i: 0.0 for i in range(self.k)}
        
        for u, v, data in self.G.edges(data=True):
            weight = data.get('weight', 1.0)
            if partition.get(u) == partition.get(v):
                internal_edges[partition[u]] += weight
        
        return internal_edges
    
    def calculate_cut_edges(self, partition: Dict[int, int]) -> float:
        """Calculate total cut edges"""
        cut_edges = 0.0
        
        for u, v, data in self.G.edges(data=True):
            weight = data.get('weight', 1.0)
            if partition.get(u) != partition.get(v):
                cut_edges += weight
        
        return cut_edges
    
    def calculate_total_edges(self) -> float:
        """Calculate total edge weights"""
        total = 0.0
        for u, v, data in self.G.edges(data=True):
            total += data.get('weight', 1.0)
        return total if total > 0 else 1.0
    
    def objective_function(self, partition: Dict[int, int]) -> float:
        """
        Calculate objective function (Eq. 19)
        f(x) = epsilon * r + (1-epsilon) * sigma_W
        """
        internal_edges = self.calculate_internal_edges(partition)
        cut_edges = self.calculate_cut_edges(partition)
        total_edges = self.calculate_total_edges()
        
        # Calculate r (cut edge ratio)
        r = cut_edges / total_edges if total_edges > 0 else 0
        
        # Calculate sigma_W (variance of internal edges)
        if self.k > 1:
            mean_internal = np.mean(list(internal_edges.values()))
            variance = np.mean([(v - mean_internal) ** 2 for v in internal_edges.values()])
            sigma_w = np.sqrt(variance)
        else:
            sigma_w = 0
        
        # Combined objective
        fitness = self.epsilon * r + (1 - self.epsilon) * sigma_w
        return fitness
    
    def get_neighbor(self, partition: Dict[int, int]) -> Dict[int, int]:
        """Generate neighbor partition by moving random task"""
        neighbor = partition.copy()
        node = random.choice(list(self.G.nodes()))
        current_subgraph = neighbor[node]
        new_subgraph = random.choice([i for i in range(self.k) if i != current_subgraph])
        neighbor[node] = new_subgraph
        return neighbor
    
    def anneal(self, T0: float = 100.0, Tf: float = 0.1, 
               cooling_rate: float = 0.95) -> Dict[int, int]:
        """
        Run simulated annealing algorithm
        """
        self.current_partition = self.initialize_partition()
        self.best_partition = self.current_partition.copy()
        self.best_fitness = self.objective_function(self.current_partition)
        
        T = T0
        iteration = 0
        
        while T > Tf:
            for _ in range(self.max_iterations):
                neighbor = self.get_neighbor(self.current_partition)
                neighbor_fitness = self.objective_function(neighbor)
                
                if neighbor_fitness < self.best_fitness:
                    self.current_partition = neighbor
                    self.best_partition = neighbor.copy()
                    self.best_fitness = neighbor_fitness
                else:
                    # Accept worse solution with probability
                    delta = neighbor_fitness - self.best_fitness
                    probability = np.exp(-delta / T) if T > 0 else 0
                    if random.random() < probability:
                        self.current_partition = neighbor
                
                # Record history for animation (limit to save memory)
                if len(self.history) < 500:
                    self.history.append({
                        'partition': self.current_partition.copy(),
                        'fitness': self.objective_function(self.current_partition),
                        'temperature': T,
                        'iteration': iteration
                    })
                iteration += 1
            
            T *= cooling_rate
        
        return self.best_partition

# ============================================================================
# VISUALIZER
# ============================================================================

class GraphPartitioningVisualizer:
    """
    Interactive visualization for graph partitioning with animation
    """
    
    def __init__(self, G: nx.DiGraph, num_partitions: int = 4):
        """Initialize visualizer"""
        self.G = G
        self.num_partitions = num_partitions
        self.partitioner = SimulatedAnnealingPartitioner(G, num_partitions, max_iterations=50)
        self.current_frame = 0
        self.is_playing = False
        self.speed = 1.0
        self.animation = None  # IMPORTANT: Store animation object
        
        # Run partitioning
        print("⏳ Running partitioning algorithm...")
        self.partition = self.partitioner.anneal()
        print(f"✓ Partitioning complete. {len(self.partitioner.history)} frames generated.")
        
        # Setup figure
        self.fig = plt.figure(figsize=(16, 10))
        self.fig.suptitle('Ra-Stream: Graph Partitioning Visualizer', 
                         fontsize=16, fontweight='bold')
        
        # Create subplots
        self.ax_graph = plt.subplot(2, 3, (1, 4))
        self.ax_metrics = plt.subplot(2, 3, 2)
        self.ax_history = plt.subplot(2, 3, 3)
        self.ax_resources = plt.subplot(2, 3, 5)
        self.ax_info = plt.subplot(2, 3, 6)
        
        # Color palette
        self.colors = sns.color_palette("husl", num_partitions)
        self.node_colors = {}
        
        # Calculate layout
        self.pos = nx.spring_layout(G, seed=42, k=0.5, iterations=50)
        
        # Setup controls
        self.setup_controls()
        
    def setup_controls(self):
        """Setup interactive controls"""
        # Play/Pause button
        ax_play = plt.axes([0.7, 0.05, 0.08, 0.04])
        self.btn_play = Button(ax_play, 'Play/Pause')
        self.btn_play.on_clicked(self.toggle_play)
        
        # Reset button
        ax_reset = plt.axes([0.79, 0.05, 0.08, 0.04])
        self.btn_reset = Button(ax_reset, 'Reset')
        self.btn_reset.on_clicked(self.reset)
        
        # Speed slider
        ax_speed = plt.axes([0.7, 0.12, 0.15, 0.03])
        self.slider_speed = Slider(ax_speed, 'Speed', 0.1, 5.0, valinit=1.0)
        self.slider_speed.on_changed(self.update_speed)
        
    def toggle_play(self, event):
        """Toggle play/pause"""
        self.is_playing = not self.is_playing
        
    def reset(self, event):
        """Reset animation"""
        self.current_frame = 0
        self.is_playing = False
        
    def update_speed(self, val):
        """Update animation speed"""
        self.speed = self.slider_speed.val
        
    def update_node_colors(self):
        """Update node colors based on partition"""
        for node in self.G.nodes():
            partition_id = self.partition.get(node, 0)
            self.node_colors[node] = self.colors[partition_id % len(self.colors)]
        
    def draw_graph(self):
        """Draw the graph with current partition"""
        self.ax_graph.clear()
        self.update_node_colors()
        
        # Draw edges
        nx.draw_networkx_edges(self.G, self.pos, ax=self.ax_graph, 
                              edge_color='gray', alpha=0.3, width=1.5, 
                              arrows=False)
        
        # Draw nodes
        node_colors = [self.node_colors[node] for node in self.G.nodes()]
        nx.draw_networkx_nodes(self.G, self.pos, ax=self.ax_graph,
                              node_color=node_colors, node_size=300,
                              edgecolors='black', linewidths=2)
        
        # Draw labels
        nx.draw_networkx_labels(self.G, self.pos, ax=self.ax_graph,
                               font_size=8, font_weight='bold')
        
        self.ax_graph.set_title('Graph Partitioning', fontweight='bold', fontsize=12)
        self.ax_graph.axis('off')
        
    def update_metrics(self):
        """Update metrics display"""
        self.ax_metrics.clear()
        
        # Calculate metrics
        internal_edges = self.partitioner.calculate_internal_edges(self.partition)
        cut_edges = self.partitioner.calculate_cut_edges(self.partition)
        total_edges = self.partitioner.calculate_total_edges()
        
        # Plot bar chart
        metrics_names = ['Internal\nEdges', 'Cut\nEdges']
        metrics_values = [sum(internal_edges.values()), cut_edges]
        colors_bar = ['green', 'red']
        
        self.ax_metrics.bar(metrics_names, metrics_values, color=colors_bar, alpha=0.7)
        self.ax_metrics.set_title('Edge Metrics', fontweight='bold', fontsize=11)
        self.ax_metrics.set_ylabel('Weight')
        self.ax_metrics.grid(axis='y', alpha=0.3)
        
    def update_history(self):
        """Update optimization history"""
        self.ax_history.clear()
        
        if len(self.partitioner.history) > 1:
            fitnesses = [h['fitness'] for h in self.partitioner.history]
            temperatures = [h['temperature'] for h in self.partitioner.history]
            
            self.ax_history.plot(fitnesses, label='Fitness', linewidth=2, color='blue')
            ax_temp = self.ax_history.twinx()
            ax_temp.plot(temperatures, label='Temperature', color='orange', linewidth=2)
            
            self.ax_history.set_xlabel('Iteration')
            self.ax_history.set_ylabel('Fitness', color='blue')
            ax_temp.set_ylabel('Temperature', color='orange')
            self.ax_history.set_title('Optimization Progress', fontweight='bold', fontsize=11)
            self.ax_history.grid(True, alpha=0.3)
            
    def update_resources(self):
        """Update resource visualization"""
        self.ax_resources.clear()
        
        # Subgraph partition distribution
        partition_counts = {}
        for subgraph_id in range(self.num_partitions):
            partition_counts[subgraph_id] = sum(1 for p in self.partition.values() 
                                               if p == subgraph_id)
        
        subgraph_ids = list(partition_counts.keys())
        counts = list(partition_counts.values())
        
        bars = self.ax_resources.bar([f'SG{i}' for i in subgraph_ids], counts, 
                                    color=self.colors, alpha=0.7, edgecolor='black')
        self.ax_resources.set_title('Task Distribution', fontweight='bold', fontsize=11)
        self.ax_resources.set_ylabel('Number of Tasks')
        self.ax_resources.grid(axis='y', alpha=0.3)
        
    def update_info(self):
        """Update information panel"""
        self.ax_info.clear()
        self.ax_info.axis('off')
        
        internal_edges_sum = sum(self.partitioner.calculate_internal_edges(self.partition).values())
        cut_edges = self.partitioner.calculate_cut_edges(self.partition)
        
        info_text = f"""PARTITIONING METRICS
{'='*45}

Nodes: {len(self.G.nodes())}
Edges: {len(self.G.edges())}
Partitions: {self.num_partitions}

Internal Edges: {internal_edges_sum:.2f}
Cut Edges: {cut_edges:.2f}
Ratio: {cut_edges/(cut_edges + internal_edges_sum):.2%}

Fitness: {self.partitioner.objective_function(self.partition):.4f}

ALGORITHM STATUS
{'='*45}
Method: Simulated Annealing
Status: {'▶ Playing' if self.is_playing else '⏸ Paused'}
Frame: {self.current_frame}/{len(self.partitioner.history)}
Speed: {self.speed:.1f}x
"""
        
        self.ax_info.text(0.02, 0.98, info_text, transform=self.ax_info.transAxes,
                         fontsize=9, verticalalignment='top', fontfamily='monospace',
                         bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))
        
    def animate(self, frame):
        """Animation update function"""
        if self.is_playing and self.current_frame < len(self.partitioner.history) - 1:
            self.current_frame += max(1, int(self.speed))
        
        if self.current_frame >= len(self.partitioner.history):
            self.current_frame = len(self.partitioner.history) - 1
        
        if len(self.partitioner.history) > 0:
            history_frame = self.partitioner.history[self.current_frame]
            self.partition = history_frame['partition'].copy()
        
        self.draw_graph()
        self.update_metrics()
        self.update_history()
        self.update_resources()
        self.update_info()
        
        return []  # Return empty list to satisfy FuncAnimation
        
    def show(self):
        """Display the visualization"""
        print("📊 Creating animation...")
        
        # IMPORTANT: Store animation in self to prevent garbage collection
        self.animation = FuncAnimation(
            self.fig, self.animate, 
            frames=max(len(self.partitioner.history), 1),
            interval=100, 
            repeat=True,
            blit=False  # Disable blit for better compatibility
        )
        
        print("✓ Animation created. Use controls to play/pause.")
        plt.tight_layout(rect=[0, 0, 1, 0.96])
        plt.show()

# ============================================================================
# EXAMPLE TOPOLOGIES
# ============================================================================

def create_wordcount_topology() -> nx.DiGraph:
    """Create WordCount stream application topology"""
    G = nx.DiGraph()
    
    nodes = [
        ('Spout', {'label': 'Spout', 'type': 'source'}),
        ('Splitter', {'label': 'Split', 'type': 'bolt'}),
        ('Counter', {'label': 'Count', 'type': 'bolt'}),
        ('Sink', {'label': 'Sink', 'type': 'sink'}),
    ]
    
    for node, attr in nodes:
        G.add_node(node, **attr)
    
    edges = [
        ('Spout', 'Splitter', {'weight': 100}),
        ('Splitter', 'Counter', {'weight': 150}),
        ('Counter', 'Sink', {'weight': 50}),
    ]
    
    for source, target, attr in edges:
        G.add_edge(source, target, **attr)
    
    return G

def create_debs_2024_topology() -> nx.DiGraph:
    """Create DEBS 2024 topology"""
    G = nx.DiGraph()
    
    nodes = [
        ('DataSource', {'label': 'Data Source', 'type': 'source'}),
        ('DataFilter', {'label': 'Filter', 'type': 'bolt'}),
        ('EventDetection', {'label': 'Event Det.', 'type': 'bolt'}),
        ('Aggregation', {'label': 'Aggreg.', 'type': 'bolt'}),
        ('DataStorage', {'label': 'Storage', 'type': 'bolt'}),
        ('AnomalyDetection', {'label': 'Anomaly', 'type': 'bolt'}),
        ('AlertSystem', {'label': 'Alert', 'type': 'bolt'}),
    ]
    
    for node, attr in nodes:
        G.add_node(node, **attr)
    
    edges = [
        ('DataSource', 'DataFilter', {'weight': 200}),
        ('DataFilter', 'EventDetection', {'weight': 180}),
        ('EventDetection', 'Aggregation', {'weight': 150}),
        ('Aggregation', 'DataStorage', {'weight': 100}),
        ('EventDetection', 'AnomalyDetection', {'weight': 120}),
        ('AnomalyDetection', 'AlertSystem', {'weight': 80}),
        ('AlertSystem', 'DataStorage', {'weight': 60}),
    ]
    
    for source, target, attr in edges:
        G.add_edge(source, target, **attr)
    
    return G

def create_custom_topology(num_nodes: int = 10, num_edges: int = 15) -> nx.DiGraph:
    """Create a custom random topology"""
    G = nx.gnp_random_graph(num_nodes, 0.3, directed=True)
    G = nx.DiGraph(G)
    
    # Add weights to edges
    for u, v in G.edges():
        G[u][v]['weight'] = random.uniform(10, 200)
    
    return G
