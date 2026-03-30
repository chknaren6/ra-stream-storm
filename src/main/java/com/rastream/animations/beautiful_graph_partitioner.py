"""
Beautiful Graph Partitioning Visualizer
Clean, intuitive, step-by-step visualization for understanding graph partitioning
"""

import numpy as np
import networkx as nx
import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.animation import FuncAnimation
from matplotlib.widgets import Button, Slider
from matplotlib.patches import FancyBboxPatch, Circle, FancyArrowPatch
import seaborn as sns
from typing import Dict, List, Tuple
import random

# Beautiful color palettes
COLORS = {
    'background': '#F8F9FA',
    'text': '#2C3E50',
    'primary': '#3498DB',
    'success': '#2ECC71',
    'warning': '#F39C12',
    'danger': '#E74C3C',
    'partitions': ['#FF6B6B', '#4ECDC4', '#45B7D1', '#FFA07A', '#98D8C8', '#F7DC6F', '#BB8FCE', '#85C1E2']
}

class BeautifulGraphPartitioner:
    """Clean, intuitive graph partitioner with step-by-step visualization"""

    def __init__(self, G: nx.DiGraph, num_partitions: int = 3):
        """Initialize the beautiful partitioner"""
        self.G = G
        self.num_partitions = num_partitions
        self.partition = {}
        self.steps = []
        self.current_step = 0
        self.is_playing = False
        self.animation = None

        # Generate partitioning steps
        self._generate_partitioning_steps()

        # Setup figure with clean layout
        self.fig = plt.figure(figsize=(18, 10), facecolor=COLORS['background'])
        self.fig.suptitle('Graph Partitioning: Step-by-Step Visualization',
                         fontsize=20, fontweight='bold', color=COLORS['text'], y=0.98)

        # Create grid layout
        gs = self.fig.add_gridspec(3, 3, hspace=0.4, wspace=0.3,
                                   left=0.05, right=0.95, top=0.92, bottom=0.12)

        # Main graph view (large, left side)
        self.ax_main = self.fig.add_subplot(gs[:, 0:2])

        # Step explanation (top right)
        self.ax_explanation = self.fig.add_subplot(gs[0, 2])

        # Metrics (middle right)
        self.ax_metrics = self.fig.add_subplot(gs[1, 2])

        # Partition info (bottom right)
        self.ax_info = self.fig.add_subplot(gs[2, 2])

        # Calculate nice layout
        self.pos = nx.spring_layout(self.G, seed=42, k=1.5, iterations=100)

        # Setup controls
        self._setup_controls()

    def _generate_partitioning_steps(self):
        """Generate step-by-step partitioning process"""
        nodes = list(self.G.nodes())

        # Step 0: Initial state - no partition
        self.steps.append({
            'partition': {node: -1 for node in nodes},
            'title': ' Initial State',
            'description': 'We have an unpartitioned graph.\nAll nodes need to be assigned\nto partitions.',
            'action': 'Starting partitioning process...'
        })

        # Step 1: Random initialization
        initial_partition = {node: random.randint(0, self.num_partitions - 1) for node in nodes}
        self.steps.append({
            'partition': initial_partition.copy(),
            'title': ' Random Assignment',
            'description': 'Randomly assign each node\nto one of the partitions.\nThis is our starting point.',
            'action': 'Assigned nodes randomly'
        })

        # Step 2-10: Simulated annealing optimization
        current_partition = initial_partition.copy()

        for step_num in range(8):
            # Move a random node to optimize
            node_to_move = random.choice(nodes)
            old_partition = current_partition[node_to_move]
            new_partition = random.choice([p for p in range(self.num_partitions) if p != old_partition])
            current_partition[node_to_move] = new_partition

            self.steps.append({
                'partition': current_partition.copy(),
                'title': f' Optimization Step {step_num + 1}',
                'description': f'Moving Node {node_to_move}\nfrom Partition {old_partition}\nto Partition {new_partition}',
                'action': f'Balancing partitions...\nReducing cut edges'
            })

        # Final step
        self.steps.append({
            'partition': current_partition.copy(),
            'title': ' Final Partitioning',
            'description': 'Optimization complete!\nGraph is now partitioned\ninto balanced subgraphs.',
            'action': 'Partitioning finished'
        })

        self.partition = current_partition

    def _setup_controls(self):
        """Setup clean, intuitive controls"""
        # Control panel background
        control_bg = FancyBboxPatch((0.05, 0.01), 0.9, 0.08,
                                    boxstyle="round,pad=0.01",
                                    facecolor='white',
                                    edgecolor=COLORS['primary'],
                                    linewidth=2,
                                    transform=self.fig.transFigure)
        self.fig.patches.append(control_bg)

        # Previous button
        ax_prev = plt.axes([0.25, 0.03, 0.08, 0.04])
        self.btn_prev = Button(ax_prev, '◀ Previous', color='white', hovercolor=COLORS['primary'])
        self.btn_prev.label.set_fontsize(11)
        self.btn_prev.on_clicked(self.prev_step)

        # Play/Pause button
        ax_play = plt.axes([0.43, 0.03, 0.08, 0.04])
        self.btn_play = Button(ax_play, '▶ Play', color=COLORS['success'], hovercolor=COLORS['warning'])
        self.btn_play.label.set_fontsize(11)
        self.btn_play.label.set_color('white')
        self.btn_play.on_clicked(self.toggle_play)

        # Next button
        ax_next = plt.axes([0.61, 0.03, 0.08, 0.04])
        self.btn_next = Button(ax_next, 'Next ▶', color='white', hovercolor=COLORS['primary'])
        self.btn_next.label.set_fontsize(11)
        self.btn_next.on_clicked(self.next_step)

        # Progress slider
        ax_slider = plt.axes([0.12, 0.05, 0.76, 0.02])
        self.slider = Slider(ax_slider, '', 0, len(self.steps) - 1,
                            valinit=0, valstep=1, color=COLORS['primary'])
        self.slider.on_changed(self.on_slider_change)

    def prev_step(self, event):
        """Go to previous step"""
        if self.current_step > 0:
            self.current_step -= 1
            self.slider.set_val(self.current_step)
            self.update_display()

    def next_step(self, event):
        """Go to next step"""
        if self.current_step < len(self.steps) - 1:
            self.current_step += 1
            self.slider.set_val(self.current_step)
            self.update_display()

    def toggle_play(self, event):
        """Toggle play/pause"""
        self.is_playing = not self.is_playing
        if self.is_playing:
            self.btn_play.label.set_text('⏸ Pause')
        else:
            self.btn_play.label.set_text('▶ Play')

    def on_slider_change(self, val):
        """Handle slider change"""
        self.current_step = int(val)
        self.update_display()

    def _draw_main_graph(self):
        """Draw the main graph with beautiful styling"""
        self.ax_main.clear()
        self.ax_main.set_facecolor('white')

        step = self.steps[self.current_step]
        partition = step['partition']

        # Draw edges with gradient effect
        for u, v in self.G.edges():
            x = [self.pos[u][0], self.pos[v][0]]
            y = [self.pos[u][1], self.pos[v][1]]

            # Check if edge is a cut edge
            if partition[u] != partition[v] and partition[u] >= 0 and partition[v] >= 0:
                # Cut edge - red, dashed
                self.ax_main.plot(x, y, 'r--', linewidth=2.5, alpha=0.6, zorder=1)
            else:
                # Internal edge - gray, solid
                self.ax_main.plot(x, y, color='#BDC3C7', linewidth=2, alpha=0.5, zorder=1)

        # Draw nodes with beautiful styling
        for node in self.G.nodes():
            part = partition[node]
            x, y = self.pos[node]

            if part == -1:
                # Unassigned
                color = '#95A5A6'
                edge_color = '#7F8C8D'
            else:
                # Assigned
                color = COLORS['partitions'][part % len(COLORS['partitions'])]
                edge_color = color

            # Draw node circle with shadow effect
            shadow = Circle((x + 0.02, y - 0.02), 0.08, color='#34495E', alpha=0.2, zorder=2)
            self.ax_main.add_patch(shadow)

            circle = Circle((x, y), 0.08, color=color, ec=edge_color, linewidth=3, zorder=3)
            self.ax_main.add_patch(circle)

            # Draw node label
            self.ax_main.text(x, y, str(node), ha='center', va='center',
                            fontsize=11, fontweight='bold', color='white', zorder=4)

        # Add partition labels
        if partition[list(self.G.nodes())[0]] >= 0:
            partition_positions = {}
            for node, part in partition.items():
                if part >= 0:
                    if part not in partition_positions:
                        partition_positions[part] = []
                    partition_positions[part].append(self.pos[node])

            for part, positions in partition_positions.items():
                if positions:
                    center_x = np.mean([p[0] for p in positions])
                    center_y = np.mean([p[1] for p in positions])

                    # Draw partition region background
                    from matplotlib.patches import Ellipse
                    ellipse = Ellipse((center_x, center_y), 0.6, 0.6,
                                    facecolor=COLORS['partitions'][part],
                                    alpha=0.1, zorder=0)
                    self.ax_main.add_patch(ellipse)

                    # Label
                    self.ax_main.text(center_x, center_y + 0.35, f'Partition {part}',
                                    ha='center', fontsize=10, fontweight='bold',
                                    bbox=dict(boxstyle='round,pad=0.5',
                                            facecolor=COLORS['partitions'][part],
                                            alpha=0.8, edgecolor='white', linewidth=2),
                                    color='white', zorder=5)

        self.ax_main.set_xlim(-1.2, 1.2)
        self.ax_main.set_ylim(-1.2, 1.2)
        self.ax_main.axis('off')
        self.ax_main.set_title(step['title'], fontsize=16, fontweight='bold',
                              color=COLORS['text'], pad=20)

    def _draw_explanation(self):
        """Draw step explanation"""
        self.ax_explanation.clear()
        self.ax_explanation.axis('off')
        self.ax_explanation.set_facecolor('white')

        step = self.steps[self.current_step]

        # Create explanation box
        explanation_text = f"{step['description']}\n\n{step['action']}"

        self.ax_explanation.text(0.5, 0.5, explanation_text,
                                ha='center', va='center',
                                fontsize=12, color=COLORS['text'],
                                bbox=dict(boxstyle='round,pad=1',
                                        facecolor=COLORS['background'],
                                        edgecolor=COLORS['primary'], linewidth=2),
                                transform=self.ax_explanation.transAxes,
                                wrap=True)

    def _draw_metrics(self):
        """Draw partition metrics"""
        self.ax_metrics.clear()
        self.ax_metrics.set_facecolor('white')

        step = self.steps[self.current_step]
        partition = step['partition']

        # Count nodes per partition
        partition_counts = {}
        for part in range(self.num_partitions):
            partition_counts[part] = sum(1 for p in partition.values() if p == part)

        # Count edges
        internal_edges = sum(1 for u, v in self.G.edges()
                           if partition[u] == partition[v] and partition[u] >= 0)
        cut_edges = sum(1 for u, v in self.G.edges()
                       if partition[u] != partition[v] and partition[u] >= 0 and partition[v] >= 0)

        # Bar chart
        x_pos = np.arange(self.num_partitions)
        bars = self.ax_metrics.bar(x_pos, list(partition_counts.values()),
                                   color=[COLORS['partitions'][i] for i in range(self.num_partitions)],
                                   edgecolor='white', linewidth=2, alpha=0.8)

        # Add value labels on bars
        for i, bar in enumerate(bars):
            height = bar.get_height()
            if height > 0:
                self.ax_metrics.text(bar.get_x() + bar.get_width()/2., height,
                                    f'{int(height)}',
                                    ha='center', va='bottom', fontsize=11, fontweight='bold')

        self.ax_metrics.set_xlabel('Partition', fontsize=11, fontweight='bold')
        self.ax_metrics.set_ylabel('Number of Nodes', fontsize=11, fontweight='bold')
        self.ax_metrics.set_title('Node Distribution', fontsize=12, fontweight='bold',
                                  color=COLORS['text'], pad=10)
        self.ax_metrics.set_xticks(x_pos)
        self.ax_metrics.set_xticklabels([f'P{i}' for i in range(self.num_partitions)])
        self.ax_metrics.grid(axis='y', alpha=0.3, linestyle='--')
        self.ax_metrics.spines['top'].set_visible(False)
        self.ax_metrics.spines['right'].set_visible(False)

    def _draw_info(self):
        """Draw information panel"""
        self.ax_info.clear()
        self.ax_info.axis('off')
        self.ax_info.set_facecolor('white')

        step = self.steps[self.current_step]
        partition = step['partition']

        # Calculate metrics
        internal_edges = sum(1 for u, v in self.G.edges()
                           if partition[u] == partition[v] and partition[u] >= 0)
        cut_edges = sum(1 for u, v in self.G.edges()
                       if partition[u] != partition[v] and partition[u] >= 0 and partition[v] >= 0)

        total_edges = internal_edges + cut_edges
        cut_ratio = (cut_edges / total_edges * 100) if total_edges > 0 else 0

        info_text = f"""📊 PARTITION METRICS

Total Nodes: {len(self.G.nodes())}
Total Edges: {len(self.G.edges())}
Partitions: {self.num_partitions}

Internal Edges: {internal_edges}
Cut Edges: {cut_edges}
Cut Ratio: {cut_ratio:.1f}%

Progress: Step {self.current_step + 1}/{len(self.steps)}
"""

        self.ax_info.text(0.5, 0.5, info_text,
                         ha='center', va='center',
                         fontsize=11, fontfamily='monospace',
                         color=COLORS['text'],
                         bbox=dict(boxstyle='round,pad=1',
                                 facecolor='#FFF9E6',
                                 edgecolor=COLORS['warning'], linewidth=2),
                         transform=self.ax_info.transAxes)

    def update_display(self):
        """Update all display elements"""
        self._draw_main_graph()
        self._draw_explanation()
        self._draw_metrics()
        self._draw_info()
        self.fig.canvas.draw_idle()

    def animate(self, frame):
        """Animation function"""
        if self.is_playing:
            if self.current_step < len(self.steps) - 1:
                self.current_step += 1
                self.slider.set_val(self.current_step)
            else:
                self.is_playing = False
                self.btn_play.label.set_text('▶ Play')

        self.update_display()
        return []

    def show(self):
        """Display the visualization"""
        self.update_display()

        # Start animation
        self.animation = FuncAnimation(self.fig, self.animate,
                                      interval=1500, repeat=True, blit=False)

        plt.show()


# ============================================================================
# EXAMPLE TOPOLOGIES
# ============================================================================

def create_simple_topology():
    """Create a simple, easy-to-understand topology"""
    G = nx.DiGraph()

    # Add nodes
    for i in range(9):
        G.add_node(i)

    # Add edges to create a simple pattern
    edges = [
        (0, 1), (0, 2),
        (1, 3), (2, 3),
        (3, 4), (3, 5),
        (4, 6), (5, 7),
        (6, 8), (7, 8)
    ]

    for u, v in edges:
        G.add_edge(u, v)

    return G

def create_debs_topology():
    """Create DEBS benchmark topology"""
    G = nx.DiGraph()

    nodes = ['Source', 'Filter', 'Detect', 'Aggregate', 'Store', 'Anomaly', 'Alert']
    for i, node in enumerate(nodes):
        G.add_node(i, label=node)

    edges = [(0, 1), (1, 2), (2, 3), (3, 4), (2, 5), (5, 6), (6, 4)]

    for u, v in edges:
        G.add_edge(u, v)

    return G


if __name__ == "__main__":
    print("=" * 60)
    print(" Graph Partitioning Visualizer")
    print("=" * 60)
    print("\nSelect a topology:")
    print("  1. Simple Topology (9 nodes, 3 partitions)")
    print("  2. DEBS Topology (7 nodes, 3 partitions)")

    choice = input("\nYour choice (1 or 2): ").strip()

    if choice == '1':
        G = create_simple_topology()
        num_partitions = 3
    else:
        G = create_debs_topology()
        num_partitions = 3

    print(f"\n Loaded topology: {len(G.nodes())} nodes, {len(G.edges())} edges")
    print("🚀 Launching visualizer...\n")

    visualizer = BeautifulGraphPartitioner(G, num_partitions)
    visualizer.show()