"""
Beautiful Resource Scaling Visualizer
Clean, intuitive visualization showing how resources scale up and down
"""

import numpy as np
import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
from matplotlib.widgets import Button, Slider
from matplotlib.patches import Rectangle, FancyBboxPatch, Circle, FancyArrowPatch
import seaborn as sns
from dataclasses import dataclass
from typing import Dict, List

# Beautiful color palette
COLORS = {
    'background': '#F8F9FA',
    'text': '#2C3E50',
    'cpu_used': '#E74C3C',
    'cpu_free': '#FFE5E5',
    'memory_used': '#3498DB',
    'memory_free': '#E3F2FD',
    'io_used': '#9B59B6',
    'io_free': '#F4ECF7',
    'active': '#2ECC71',
    'idle': '#BDC3C7',
    'warning': '#F39C12',
    'success': '#27AE60'
}

@dataclass
class ServerState:
    """State of a compute server"""
    id: int
    cpu_total: float
    cpu_used: float
    memory_total: float
    memory_used: float
    io_total: float
    io_used: float
    is_active: bool
    num_tasks: int

@dataclass
class ScalingStep:
    """A step in the scaling process"""
    step_num: int
    title: str
    description: str
    servers: List[ServerState]
    action: str
    workload_level: str  # 'low', 'normal', 'high', 'overload'

class BeautifulResourceScaler:
    """Beautiful resource scaling visualizer"""

    def __init__(self, max_servers: int = 8):
        """Initialize the visualizer"""
        self.max_servers = max_servers
        self.current_step = 0
        self.is_playing = False
        self.animation = None

        # Generate scaling scenario
        self.steps = self._generate_scaling_scenario()

        # Setup figure
        self.fig = plt.figure(figsize=(18, 10), facecolor=COLORS['background'])
        self.fig.suptitle('Resource Scaling: Step-by-Step Visualization',
                         fontsize=20, fontweight='bold', color=COLORS['text'], y=0.98)

        # Create grid layout
        gs = self.fig.add_gridspec(3, 3, hspace=0.4, wspace=0.3,
                                   left=0.05, right=0.95, top=0.92, bottom=0.12)

        # Main server view (large, left side)
        self.ax_servers = self.fig.add_subplot(gs[:, 0:2])

        # Explanation (top right)
        self.ax_explanation = self.fig.add_subplot(gs[0, 2])

        # Workload gauge (middle right)
        self.ax_gauge = self.fig.add_subplot(gs[1, 2])

        # Metrics (bottom right)
        self.ax_metrics = self.fig.add_subplot(gs[2, 2])

        # Setup controls
        self._setup_controls()

    def _generate_scaling_scenario(self) -> List[ScalingStep]:
        """Generate a step-by-step scaling scenario"""
        steps = []

        # Step 0: Initial state - normal load
        servers = [
            ServerState(1, 8, 4.0, 16, 8.0, 100, 50, True, 3),
            ServerState(2, 8, 4.2, 16, 8.5, 100, 52, True, 3),
            ServerState(3, 8, 3.8, 16, 7.5, 100, 48, True, 2),
        ] + [ServerState(i, 8, 0, 16, 0, 100, 0, False, 0) for i in range(4, self.max_servers + 1)]

        steps.append(ScalingStep(
            0, '🟢 Normal Operation',
            'System running normally\nwith 3 active servers.\nWorkload is balanced.',
            servers.copy(), 'Normal operation', 'normal'
        ))

        # Step 1: Workload starts increasing
        servers = [
            ServerState(1, 8, 5.5, 16, 11.0, 100, 68, True, 4),
            ServerState(2, 8, 5.8, 16, 11.5, 100, 72, True, 4),
            ServerState(3, 8, 5.2, 16, 10.5, 100, 65, True, 4),
        ] + [ServerState(i, 8, 0, 16, 0, 100, 0, False, 0) for i in range(4, self.max_servers + 1)]

        steps.append(ScalingStep(
            1, '⚠️ Workload Increasing',
            'Data stream rate increasing!\nServers reaching 70% capacity.\nTime to scale up.',
            servers.copy(), 'Workload increasing', 'high'
        ))

        # Step 2: System detects overload
        servers = [
            ServerState(1, 8, 6.8, 16, 13.5, 100, 85, True, 5),
            ServerState(2, 8, 7.0, 16, 14.0, 100, 88, True, 5),
            ServerState(3, 8, 6.5, 16, 13.0, 100, 82, True, 5),
        ] + [ServerState(i, 8, 0, 16, 0, 100, 0, False, 0) for i in range(4, self.max_servers + 1)]

        steps.append(ScalingStep(
            2, ' Overload Detected!',
            'Servers at 85% capacity!\nPerformance degrading.\nScaling action needed NOW.',
            servers.copy(), 'Overload detected', 'overload'
        ))

        # Step 3: Adding new server
        servers = [
            ServerState(1, 8, 6.8, 16, 13.5, 100, 85, True, 5),
            ServerState(2, 8, 7.0, 16, 14.0, 100, 88, True, 5),
            ServerState(3, 8, 6.5, 16, 13.0, 100, 82, True, 5),
            ServerState(4, 8, 0.5, 16, 1.0, 100, 5, True, 1),  # New server starting
        ] + [ServerState(i, 8, 0, 16, 0, 100, 0, False, 0) for i in range(5, self.max_servers + 1)]

        steps.append(ScalingStep(
            3, '📈 Scaling Up (1/2)',
            'Adding Server 4...\nProvisioning resources.\nMigrating some tasks.',
            servers.copy(), 'Scale up: Adding server', 'overload'
        ))

        # Step 4: Adding another server
        servers = [
            ServerState(1, 8, 5.2, 16, 10.5, 100, 65, True, 4),
            ServerState(2, 8, 5.4, 16, 11.0, 100, 68, True, 4),
            ServerState(3, 8, 5.0, 16, 10.0, 100, 62, True, 4),
            ServerState(4, 8, 2.5, 16, 5.0, 100, 30, True, 2),
            ServerState(5, 8, 0.5, 16, 1.0, 100, 5, True, 1),  # Another new server
        ] + [ServerState(i, 8, 0, 16, 0, 100, 0, False, 0) for i in range(6, self.max_servers + 1)]

        steps.append(ScalingStep(
            4, '📈 Scaling Up (2/2)',
            'Adding Server 5...\nLoad redistributing.\nSystem stabilizing.',
            servers.copy(), 'Scale up: Adding server', 'high'
        ))

        # Step 5: Load balanced after scale up
        servers = [
            ServerState(1, 8, 4.5, 16, 9.0, 100, 56, True, 3),
            ServerState(2, 8, 4.6, 16, 9.2, 100, 58, True, 3),
            ServerState(3, 8, 4.4, 16, 8.8, 100, 55, True, 3),
            ServerState(4, 8, 4.3, 16, 8.6, 100, 54, True, 3),
            ServerState(5, 8, 4.2, 16, 8.4, 100, 52, True, 3),
        ] + [ServerState(i, 8, 0, 16, 0, 100, 0, False, 0) for i in range(6, self.max_servers + 1)]

        steps.append(ScalingStep(
            5, 'Balanced After Scale Up',
            'Load now distributed across\n5 servers. Each at ~55%.\nPerformance restored!',
            servers.copy(), 'Balanced after scale up', 'normal'
        ))

        # Step 6: Workload decreases
        servers = [
            ServerState(1, 8, 2.5, 16, 5.0, 100, 31, True, 2),
            ServerState(2, 8, 2.6, 16, 5.2, 100, 32, True, 2),
            ServerState(3, 8, 2.4, 16, 4.8, 100, 30, True, 2),
            ServerState(4, 8, 2.3, 16, 4.6, 100, 29, True, 2),
            ServerState(5, 8, 2.2, 16, 4.4, 100, 28, True, 2),
        ] + [ServerState(i, 8, 0, 16, 0, 100, 0, False, 0) for i in range(6, self.max_servers + 1)]

        steps.append(ScalingStep(
            6, '📉 Workload Decreasing',
            'Stream rate dropping.\nServers only at 30% capacity.\nCan reduce resources.',
            servers.copy(), 'Workload decreasing', 'low'
        ))

        # Step 7: Removing a server
        servers = [
            ServerState(1, 8, 3.0, 16, 6.0, 100, 38, True, 3),
            ServerState(2, 8, 3.1, 16, 6.2, 100, 39, True, 3),
            ServerState(3, 8, 2.9, 16, 5.8, 100, 36, True, 2),
            ServerState(4, 8, 2.8, 16, 5.6, 100, 35, True, 2),
            ServerState(5, 8, 0.2, 16, 0.5, 100, 3, True, 0),  # Being decommissioned
        ] + [ServerState(i, 8, 0, 16, 0, 100, 0, False, 0) for i in range(6, self.max_servers + 1)]

        steps.append(ScalingStep(
            7, '📉 Scaling Down (1/2)',
            'Removing Server 5...\nMigrating remaining tasks.\nReducing costs.',
            servers.copy(), 'Scale down: Removing server', 'low'
        ))

        # Step 8: Removing another server
        servers = [
            ServerState(1, 8, 3.8, 16, 7.6, 100, 48, True, 3),
            ServerState(2, 8, 3.9, 16, 7.8, 100, 49, True, 3),
            ServerState(3, 8, 3.7, 16, 7.4, 100, 46, True, 3),
            ServerState(4, 8, 0.2, 16, 0.5, 100, 3, True, 0),  # Being decommissioned
        ] + [ServerState(i, 8, 0, 16, 0, 100, 0, False, 0) for i in range(5, self.max_servers + 1)]

        steps.append(ScalingStep(
            8, '📉 Scaling Down (2/2)',
            'Removing Server 4...\nTask redistribution.\nOptimizing efficiency.',
            servers.copy(), 'Scale down: Removing server', 'low'
        ))

        # Step 9: Final balanced state
        servers = [
            ServerState(1, 8, 4.0, 16, 8.0, 100, 50, True, 3),
            ServerState(2, 8, 4.2, 16, 8.4, 100, 52, True, 3),
            ServerState(3, 8, 3.8, 16, 7.6, 100, 48, True, 3),
        ] + [ServerState(i, 8, 0, 16, 0, 100, 0, False, 0) for i in range(4, self.max_servers + 1)]

        steps.append(ScalingStep(
            9, '🟢 Back to Optimal',
            'System optimized for current\nworkload. 3 servers at 50%.\nCost efficient!',
            servers.copy(), 'Optimized', 'normal'
        ))

        return steps

    def _setup_controls(self):
        """Setup clean controls"""
        # Control panel background
        control_bg = FancyBboxPatch((0.05, 0.01), 0.9, 0.08,
                                    boxstyle="round,pad=0.01",
                                    facecolor='white',
                                    edgecolor=COLORS['cpu_used'],
                                    linewidth=2,
                                    transform=self.fig.transFigure)
        self.fig.patches.append(control_bg)

        # Previous button
        ax_prev = plt.axes([0.25, 0.03, 0.08, 0.04])
        self.btn_prev = Button(ax_prev, '◀ Previous', color='white', hovercolor=COLORS['cpu_used'])
        self.btn_prev.label.set_fontsize(11)
        self.btn_prev.on_clicked(self.prev_step)

        # Play/Pause button
        ax_play = plt.axes([0.43, 0.03, 0.08, 0.04])
        self.btn_play = Button(ax_play, '▶ Play', color=COLORS['active'], hovercolor=COLORS['warning'])
        self.btn_play.label.set_fontsize(11)
        self.btn_play.label.set_color('white')
        self.btn_play.on_clicked(self.toggle_play)

        # Next button
        ax_next = plt.axes([0.61, 0.03, 0.08, 0.04])
        self.btn_next = Button(ax_next, 'Next ▶', color='white', hovercolor=COLORS['cpu_used'])
        self.btn_next.label.set_fontsize(11)
        self.btn_next.on_clicked(self.next_step)

        # Progress slider
        ax_slider = plt.axes([0.12, 0.05, 0.76, 0.02])
        self.slider = Slider(ax_slider, '', 0, len(self.steps) - 1,
                            valinit=0, valstep=1, color=COLORS['cpu_used'])
        self.slider.on_changed(self.on_slider_change)

    def prev_step(self, event):
        if self.current_step > 0:
            self.current_step -= 1
            self.slider.set_val(self.current_step)
            self.update_display()

    def next_step(self, event):
        if self.current_step < len(self.steps) - 1:
            self.current_step += 1
            self.slider.set_val(self.current_step)
            self.update_display()

    def toggle_play(self, event):
        self.is_playing = not self.is_playing
        if self.is_playing:
            self.btn_play.label.set_text('⏸ Pause')
        else:
            self.btn_play.label.set_text('▶ Play')

    def on_slider_change(self, val):
        self.current_step = int(val)
        self.update_display()

    def _draw_servers(self):
        """Draw server resource visualization"""
        self.ax_servers.clear()
        self.ax_servers.set_facecolor('white')

        step = self.steps[self.current_step]
        servers = [s for s in step.servers if s.is_active or s.id <= 5]  # Show first 5 slots

        bar_height = 0.6
        spacing = 1.2
        y_start = len(servers) * spacing

        for idx, server in enumerate(servers):
            y_pos = y_start - (idx * spacing)

            # Server label
            status_emoji = '🟢' if server.is_active else '⚪'
            self.ax_servers.text(-1.5, y_pos, f'{status_emoji} Server {server.id}',
                                ha='right', va='center', fontsize=12, fontweight='bold',
                                color=COLORS['text'])

            if server.is_active:
                # CPU bar
                cpu_usage = (server.cpu_used / server.cpu_total) * 10
                self.ax_servers.barh(y_pos + 0.3, server.cpu_total, bar_height * 0.4,
                                    left=0, color=COLORS['cpu_free'], edgecolor='black', linewidth=1)
                self.ax_servers.barh(y_pos + 0.3, cpu_usage, bar_height * 0.4,
                                    left=0, color=COLORS['cpu_used'], edgecolor='black', linewidth=1)
                self.ax_servers.text(-0.3, y_pos + 0.3, 'CPU', ha='right', va='center',
                                    fontsize=9, fontweight='bold')
                self.ax_servers.text(cpu_usage + 0.2, y_pos + 0.3,
                                    f'{server.cpu_used:.1f}/{server.cpu_total}',
                                    ha='left', va='center', fontsize=8)

                # Memory bar
                mem_usage = (server.memory_used / server.memory_total) * 10
                self.ax_servers.barh(y_pos, bar_height * 0.4,
                                    left=0, color=COLORS['memory_free'], edgecolor='black', linewidth=1)
                self.ax_servers.barh(y_pos, mem_usage, bar_height * 0.4,
                                    left=0, color=COLORS['memory_used'], edgecolor='black', linewidth=1)
                self.ax_servers.text(-0.3, y_pos, 'RAM', ha='right', va='center',
                                    fontsize=9, fontweight='bold')
                self.ax_servers.text(mem_usage + 0.2, y_pos,
                                    f'{server.memory_used:.1f}/{server.memory_total}',
                                    ha='left', va='center', fontsize=8)

                # I/O bar
                io_usage = (server.io_used / server.io_total) * 10
                self.ax_servers.barh(y_pos - 0.3, bar_height * 0.4,
                                    left=0, color=COLORS['io_free'], edgecolor='black', linewidth=1)
                self.ax_servers.barh(y_pos - 0.3, io_usage, bar_height * 0.4,
                                    left=0, color=COLORS['io_used'], edgecolor='black', linewidth=1)
                self.ax_servers.text(-0.3, y_pos - 0.3, 'I/O', ha='right', va='center',
                                    fontsize=9, fontweight='bold')
                self.ax_servers.text(io_usage + 0.2, y_pos - 0.3,
                                    f'{server.io_used:.0f}/{server.io_total}',
                                    ha='left', va='center', fontsize=8)

                # Task count
                self.ax_servers.text(11.5, y_pos, f'📦 {server.num_tasks} tasks',
                                    ha='left', va='center', fontsize=10,
                                    bbox=dict(boxstyle='round,pad=0.3', facecolor='lightyellow'))
            else:
                # Inactive server
                self.ax_servers.text(5, y_pos, 'IDLE', ha='center', va='center',
                                    fontsize=14, color=COLORS['idle'], fontweight='bold')

        self.ax_servers.set_xlim(-2, 14)
        self.ax_servers.set_ylim(-0.5, y_start + 0.5)
        self.ax_servers.axis('off')
        self.ax_servers.set_title(step.title, fontsize=16, fontweight='bold',
                                  color=COLORS['text'], pad=20)

    def _draw_explanation(self):
        """Draw explanation"""
        self.ax_explanation.clear()
        self.ax_explanation.axis('off')
        self.ax_explanation.set_facecolor('white')

        step = self.steps[self.current_step]

        self.ax_explanation.text(0.5, 0.5, step.description,
                                ha='center', va='center',
                                fontsize=12, color=COLORS['text'],
                                bbox=dict(boxstyle='round,pad=1',
                                        facecolor=COLORS['background'],
                                        edgecolor=COLORS['cpu_used'], linewidth=2),
                                transform=self.ax_explanation.transAxes)

    def _draw_gauge(self):
        """Draw workload gauge"""
        self.ax_gauge.clear()
        self.ax_gauge.set_facecolor('white')
        self.ax_gauge.axis('off')

        step = self.steps[self.current_step]

        # Calculate average utilization
        active_servers = [s for s in step.servers if s.is_active]
        if active_servers:
            avg_util = np.mean([s.cpu_used / s.cpu_total for s in active_servers]) * 100
        else:
            avg_util = 0

        # Draw gauge
        angles = np.linspace(0, np.pi, 100)
        x = 0.5 + 0.35 * np.cos(angles)
        y = 0.2 + 0.35 * np.sin(angles)
        self.ax_gauge.plot(x, y, 'k-', linewidth=3, transform=self.ax_gauge.transAxes)

        # Color zones
        zone_colors = [
            (0, 30, COLORS['active']),  # Green zone
            (30, 80, COLORS['warning']),  # Yellow zone
            (80, 100, COLORS['cpu_used'])  # Red zone
        ]

        for start, end, color in zone_colors:
            start_angle = np.pi * (1 - start/100)
            end_angle = np.pi * (1 - end/100)
            angles_zone = np.linspace(start_angle, end_angle, 20)
            x_zone = 0.5 + 0.35 * np.cos(angles_zone)
            y_zone = 0.2 + 0.35 * np.sin(angles_zone)
            self.ax_gauge.plot(x_zone, y_zone, color=color, linewidth=8, alpha=0.7,
                             transform=self.ax_gauge.transAxes)

        # Needle
        needle_angle = np.pi * (1 - avg_util/100)
        needle_x = [0.5, 0.5 + 0.3 * np.cos(needle_angle)]
        needle_y = [0.2, 0.2 + 0.3 * np.sin(needle_angle)]
        self.ax_gauge.plot(needle_x, needle_y, 'k-', linewidth=3,
                          transform=self.ax_gauge.transAxes)
        self.ax_gauge.plot([0.5], [0.2], 'ko', markersize=10,
                          transform=self.ax_gauge.transAxes)

        # Labels
        self.ax_gauge.text(0.5, 0.7, 'Workload Level', ha='center', fontsize=12,
                          fontweight='bold', transform=self.ax_gauge.transAxes)
        self.ax_gauge.text(0.5, 0.5, f'{avg_util:.0f}%', ha='center', fontsize=24,
                          fontweight='bold', color=COLORS['cpu_used'],
                          transform=self.ax_gauge.transAxes)

    def _draw_metrics(self):
        """Draw metrics panel"""
        self.ax_metrics.clear()
        self.ax_metrics.axis('off')
        self.ax_metrics.set_facecolor('white')

        step = self.steps[self.current_step]

        active_count = sum(1 for s in step.servers if s.is_active)
        total_tasks = sum(s.num_tasks for s in step.servers if s.is_active)

        if active_count > 0:
            avg_cpu = np.mean([s.cpu_used / s.cpu_total * 100 for s in step.servers if s.is_active])
            avg_mem = np.mean([s.memory_used / s.memory_total * 100 for s in step.servers if s.is_active])
        else:
            avg_cpu = avg_mem = 0

        info_text = f"""📊 SYSTEM METRICS

Active Servers: {active_count}/{self.max_servers}
Total Tasks: {total_tasks}

Avg CPU: {avg_cpu:.1f}%
Avg Memory: {avg_mem:.1f}%

Status: {step.action}

Step: {self.current_step + 1}/{len(self.steps)}
"""

        self.ax_metrics.text(0.5, 0.5, info_text,
                            ha='center', va='center',
                            fontsize=11, fontfamily='monospace',
                            color=COLORS['text'],
                            bbox=dict(boxstyle='round,pad=1',
                                    facecolor='#FFF9E6',
                                    edgecolor=COLORS['warning'], linewidth=2),
                            transform=self.ax_metrics.transAxes)

    def update_display(self):
        """Update all displays"""
        self._draw_servers()
        self._draw_explanation()
        self._draw_gauge()
        self._draw_metrics()
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
                                      interval=2000, repeat=True, blit=False)

        plt.show()


if __name__ == "__main__":
    print("=" * 60)
    print(" Resource Scaling Visualizer")
    print("=" * 60)
    print("\n Launching visualizer...")
    print("\nThis shows how a distributed system scales resources")
    print("up and down based on workload demands.\n")

    scaler = BeautifulResourceScaler(max_servers=8)
    scaler.show()