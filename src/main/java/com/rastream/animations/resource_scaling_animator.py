"""
Resource Scaling Animator
Visualizes resource scaling operations (shrink/extend) for stream applications
"""

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.animation import FuncAnimation
from matplotlib.widgets import Slider, Button
from dataclasses import dataclass
from typing import Dict, List, Tuple
import seaborn as sns

@dataclass
class ResourceSnapshot:
    """Snapshot of resource state at a point in time"""
    timestamp: int
    compute_nodes: Dict[int, Dict]  # {node_id: {cpu, memory, io, subgraphs}}
    subgraphs: Dict[int, Dict]  # {subgraph_id: {cpu_req, mem_req, io_req}}
    action: str  # 'extend', 'shrink', 'balanced'

class ResourceScalingAnimator:
    """Animate resource scaling operations"""
    
    def __init__(self, num_nodes: int = 10, num_subgraphs: int = 8):
        """
        Initialize animator
        
        Args:
            num_nodes: Number of compute nodes
            num_subgraphs: Number of subgraphs
        """
        self.num_nodes = num_nodes
        self.num_subgraphs = num_subgraphs
        self.current_frame = 0
        self.is_playing = False
        self.speed = 1.0
        
        # Generate synthetic data
        self.snapshots = self.generate_scaling_scenario()
        
        # Setup figure
        self.fig = plt.figure(figsize=(16, 10))
        self.fig.suptitle('Ra-Stream: Resource Scaling Visualizer', fontsize=16, fontweight='bold')
        
        # Create subplots
        self.ax_nodes = plt.subplot(2, 3, (1, 4))
        self.ax_cpu = plt.subplot(2, 3, 2)
        self.ax_memory = plt.subplot(2, 3, 3)
        self.ax_io = plt.subplot(2, 3, 5)
        self.ax_info = plt.subplot(2, 3, 6)
        
        # Colors
        self.node_colors = sns.color_palette("husl", num_subgraphs)
        
        # Setup controls
        self.setup_controls()
        
    def generate_scaling_scenario(self) -> List[ResourceSnapshot]:
        """Generate synthetic resource scaling scenario"""
        snapshots = []
        underload_threshold = 0.3
        overload_threshold = 0.8
        
        # Scenario: Start with overload, then scale extend, then scale shrink
        scenarios = [
            # Phase 1: Overload (high data stream rate)
            ('overload', 0.85, [1, 2, 3, 4, 5, 6, 7, 8]),
            ('overload', 0.87, [1, 2, 3, 4, 5, 6, 7, 8]),
            # Phase 2: Scale extend (add more nodes)
            ('extend', 0.70, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
            ('extend', 0.65, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
            # Phase 3: Balanced
            ('balanced', 0.55, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
            # Phase 4: Stream rate decreases - underload
            ('underload', 0.35, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
            ('underload', 0.30, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
            # Phase 5: Scale shrink
            ('shrink', 0.45, [1, 2, 3, 4, 5, 6, 7]),
            ('shrink', 0.50, [1, 2, 3, 4, 5, 6]),
            # Phase 6: Back to balanced
            ('balanced', 0.55, [1, 2, 3, 4, 5, 6]),
        ]
        
        for timestamp, (action, avg_utilization, active_nodes) in enumerate(scenarios):
            compute_nodes = {}
            
            for node_id in range(1, self.num_nodes + 1):
                if node_id in active_nodes:
                    # Active node
                    utilization = avg_utilization + np.random.uniform(-0.05, 0.05)
                    cpu_used = 8 * utilization  # 8 CPU cores
                    mem_used = 16 * utilization  # 16 GB RAM
                    io_used = 100 * utilization  # 100 Mbps
                    num_subgraphs = np.random.randint(1, 4)
                else:
                    # Idle node
                    cpu_used = 0
                    mem_used = 0
                    io_used = 0
                    num_subgraphs = 0
                
                compute_nodes[node_id] = {
                    'cpu_total': 8,
                    'cpu_used': cpu_used,
                    'memory_total': 16,
                    'memory_used': mem_used,
                    'io_total': 100,
                    'io_used': io_used,
                    'num_subgraphs': num_subgraphs,
                    'status': 'active' if node_id in active_nodes else 'idle'
                }
            
            # Generate subgraph requirements
            subgraphs = {}
            for i in range(self.num_subgraphs):
                if i < len(active_nodes) * 1.5:  # Distribute subgraphs
                    cpu_req = np.random.uniform(0.5, 2.0)
                    mem_req = np.random.uniform(1, 4)
                    io_req = np.random.uniform(5, 25)
                else:
                    cpu_req = mem_req = io_req = 0
                
                subgraphs[i] = {
                    'cpu_requirement': cpu_req,
                    'memory_requirement': mem_req,
                    'io_requirement': io_req
                }
            
            snapshot = ResourceSnapshot(
                timestamp=timestamp,
                compute_nodes=compute_nodes,
                subgraphs=subgraphs,
                action=action
            )
            snapshots.append(snapshot)
        
        return snapshots
    
    def setup_controls(self):
        """Setup interactive controls"""
        ax_play = plt.axes([0.7, 0.05, 0.08, 0.04])
        self.btn_play = Button(ax_play, 'Play/Pause')
        self.btn_play.on_clicked(self.toggle_play)
        
        ax_reset = plt.axes([0.79, 0.05, 0.08, 0.04])
        self.btn_reset = Button(ax_reset, 'Reset')
        self.btn_reset.on_clicked(self.reset)
        
        ax_speed = plt.axes([0.7, 0.12, 0.15, 0.03])
        self.slider_speed = Slider(ax_speed, 'Speed', 0.1, 5.0, valinit=1.0)
        self.slider_speed.on_changed(self.update_speed)
    
    def toggle_play(self, event):
        self.is_playing = not self.is_playing
    
    def reset(self, event):
        self.current_frame = 0
        self.is_playing = False
    
    def update_speed(self, val):
        self.speed = self.slider_speed.val
    
    def draw_nodes(self, snapshot: ResourceSnapshot):
        """Draw compute node resources"""
        self.ax_nodes.clear()
        
        nodes_data = snapshot.compute_nodes
        node_ids = sorted(nodes_data.keys())
        
        y_pos = 0
        bar_height = 0.8
        
        for node_id in node_ids:
            node = nodes_data[node_id]
            color = 'lightgreen' if node['status'] == 'active' else 'lightgray'
            
            # Draw background
            self.ax_nodes.barh(y_pos, node['cpu_total'], bar_height, 
                              color='lightblue', edgecolor='black', linewidth=1.5)
            
            # Draw used portion
            self.ax_nodes.barh(y_pos, node['cpu_used'], bar_height,
                              color=color, edgecolor='black', linewidth=1.5)
            
            # Add label
            utilization = (node['cpu_used'] / node['cpu_total'] * 100) if node['cpu_total'] > 0 else 0
            label_text = f"Node {node_id}: {utilization:.0f}% ({node['num_subgraphs']} SGs)"
            self.ax_nodes.text(-0.5, y_pos, label_text, ha='right', va='center', fontsize=9)
            
            y_pos += 1
        
        self.ax_nodes.set_xlim(-3, 10)
        self.ax_nodes.set_ylim(-1, len(node_ids))
        self.ax_nodes.set_xlabel('CPU Usage (cores)')
        self.ax_nodes.set_title('Compute Node Resource Utilization', fontweight='bold')
        self.ax_nodes.set_yticks([])
        self.ax_nodes.grid(axis='x', alpha=0.3)
    
    def draw_cpu_distribution(self, snapshot: ResourceSnapshot):
        """Draw CPU distribution"""
        self.ax_cpu.clear()
        
        nodes_data = snapshot.compute_nodes
        node_ids = sorted([nid for nid in nodes_data.keys() if nodes_data[nid]['status'] == 'active'])
        
        cpu_used = [nodes_data[nid]['cpu_used'] for nid in node_ids]
        cpu_available = [nodes_data[nid]['cpu_total'] - nodes_data[nid]['cpu_used'] for nid in node_ids]
        
        x = np.arange(len(node_ids))
        
        self.ax_cpu.bar(x, cpu_used, label='Used', color='#FF6B6B', alpha=0.8)
        self.ax_cpu.bar(x, cpu_available, bottom=cpu_used, label='Available', 
                       color='#4ECDC4', alpha=0.8)
        
        self.ax_cpu.set_ylabel('CPU (cores)')
        self.ax_cpu.set_title('CPU Distribution', fontweight='bold')
        self.ax_cpu.set_xticks(x)
        self.ax_cpu.set_xticklabels([f'N{nid}' for nid in node_ids], fontsize=8)
        self.ax_cpu.legend(loc='upper left', fontsize=8)
        self.ax_cpu.grid(axis='y', alpha=0.3)
    
    def draw_memory_distribution(self, snapshot: ResourceSnapshot):
        """Draw memory distribution"""
        self.ax_memory.clear()
        
        nodes_data = snapshot.compute_nodes
        node_ids = sorted([nid for nid in nodes_data.keys() if nodes_data[nid]['status'] == 'active'])
        
        mem_used = [nodes_data[nid]['memory_used'] for nid in node_ids]
        mem_available = [nodes_data[nid]['memory_total'] - nodes_data[nid]['memory_used'] for nid in node_ids]
        
        x = np.arange(len(node_ids))
        
        self.ax_memory.bar(x, mem_used, label='Used', color='#95E1D3', alpha=0.8)
        self.ax_memory.bar(x, mem_available, bottom=mem_used, label='Available',
                          color='#F38181', alpha=0.8)
        
        self.ax_memory.set_ylabel('Memory (GB)')
        self.ax_memory.set_title('Memory Distribution', fontweight='bold')
        self.ax_memory.set_xticks(x)
        self.ax_memory.set_xticklabels([f'N{nid}' for nid in node_ids], fontsize=8)
        self.ax_memory.legend(loc='upper left', fontsize=8)
        self.ax_memory.grid(axis='y', alpha=0.3)
    
    def draw_io_distribution(self, snapshot: ResourceSnapshot):
        """Draw I/O distribution"""
        self.ax_io.clear()
        
        nodes_data = snapshot.compute_nodes
        node_ids = sorted([nid for nid in nodes_data.keys() if nodes_data[nid]['status'] == 'active'])
        
        io_used = [nodes_data[nid]['io_used'] for nid in node_ids]
        io_available = [nodes_data[nid]['io_total'] - nodes_data[nid]['io_used'] for nid in node_ids]
        
        x = np.arange(len(node_ids))
        
        self.ax_io.bar(x, io_used, label='Used', color='#AA96DA', alpha=0.8)
        self.ax_io.bar(x, io_available, bottom=io_used, label='Available',
                      color='#FCBAD3', alpha=0.8)
        
        self.ax_io.set_ylabel('I/O (Mbps)')
        self.ax_io.set_title('I/O Distribution', fontweight='bold')
        self.ax_io.set_xticks(x)
        self.ax_io.set_xticklabels([f'N{nid}' for nid in node_ids], fontsize=8)
        self.ax_io.legend(loc='upper left', fontsize=8)
        self.ax_io.grid(axis='y', alpha=0.3)
    
    def draw_info(self, snapshot: ResourceSnapshot):
        """Draw information panel"""
        self.ax_info.clear()
        self.ax_info.axis('off')
        
        # Calculate aggregated metrics
        active_nodes = [n for n in snapshot.compute_nodes.values() if n['status'] == 'active']
        total_cpu_used = sum(n['cpu_used'] for n in active_nodes)
        total_cpu_available = sum(n['cpu_total'] for n in active_nodes)
        avg_utilization = (total_cpu_used / total_cpu_available * 100) if total_cpu_available > 0 else 0
        
        action_descriptions = {
            'extend': '📈 EXTENDING RESOURCES\n(Stream rate increasing)',
            'shrink': '📉 SHRINKING RESOURCES\n(Stream rate decreasing)',
            'balanced': '⚖️  BALANCED\n(Normal operation)',
            'overload': '⚠️  OVERLOADED\n(Scaling needed)',
            'underload': '📊 UNDERLOADED\n(Can be merged)'
        }
        
        info_text = f"""
        RESOURCE SCALING METRICS
        {'='*50}
        
        Active Compute Nodes: {len(active_nodes)}/{self.num_nodes}
        Total Subgraphs: {sum(n['num_subgraphs'] for n in active_nodes)}
        
        CPU Utilization: {total_cpu_used:.1f}/{total_cpu_available:.1f} cores
        Average Load: {avg_utilization:.1f}%
        
        THRESHOLDS
        {'='*50}
        Underload Threshold: < 30%
        Balanced Range: 30-80%
        Overload Threshold: > 80%
        
        CURRENT ACTION
        {'='*50}
        {action_descriptions.get(snapshot.action, 'UNKNOWN')}
        
        Time: {snapshot.timestamp}s
        """
        
        self.ax_info.text(0.05, 0.95, info_text, transform=self.ax_info.transAxes,
                         fontsize=9, verticalalignment='top', fontfamily='monospace',
                         bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.7))
    
    def animate(self, frame):
        """Animation update"""
        if self.is_playing and self.current_frame < len(self.snapshots) - 1:
            self.current_frame += max(1, int(self.speed))
        
        if self.current_frame >= len(self.snapshots):
            self.current_frame = len(self.snapshots) - 1
        
        snapshot = self.snapshots[self.current_frame]
        
        self.draw_nodes(snapshot)
        self.draw_cpu_distribution(snapshot)
        self.draw_memory_distribution(snapshot)
        self.draw_io_distribution(snapshot)
        self.draw_info(snapshot)
    
    def show(self):
        """Display visualization"""
        ani = FuncAnimation(self.fig, self.animate, frames=len(self.snapshots),
                          interval=500, repeat=True)
        plt.tight_layout()
        plt.show()

if __name__ == "__main__":
    animator = ResourceScalingAnimator(num_nodes=10, num_subgraphs=8)
    animator.show()
