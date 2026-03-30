"""
Main Launcher for Beautiful Visualizers
Easy-to-use menu for graph partitioning and resource scaling visualizations
"""

import sys
import os

def clear_screen():
    """Clear the terminal screen"""
    os.system('cls' if os.name == 'nt' else 'clear')

def print_banner():
    """Print welcome banner"""
    banner = """
╔═══════════════════════════════════════════════════════════╗
║                                                           ║
║     DISTRIBUTED SYSTEMS VISUALIZATION SUITE               ║
║                                                           ║
║                                                           ║
║               Naren Chintakindi                           ║
║                                                           ║
╚═══════════════════════════════════════════════════════════╝
    """
    print(banner)

def print_main_menu():
    """Print the main menu"""
    print("\n" + "="*60)
    print("  SELECT A VISUALIZATION")
    print("="*60 + "\n")

    print("  1. * Graph Partitioning Visualizer")
    print("     └─ See how graphs are divided into partitions")
    print("     └─ Understand cut edges vs internal edges")
    print("     └─ Watch optimization algorithms in action")
    print()

    print("  2. *  Resource Scaling Visualizer")
    print("     └─ Watch servers scale up and down")
    print("     └─ Understand load balancing")
    print("     └─ See resource utilization in real-time")
    print()

    print("  3. * View Quick Tutorial")
    print("     └─ Learn how to use the visualizers")
    print()

    print("  4.  Exit")
    print()

def print_tutorial():
    """Print tutorial information"""
    clear_screen()
    print("\n" + "="*60)
    print("  📚 QUICK TUTORIAL")
    print("="*60 + "\n")

    print("""
HOW TO USE THE VISUALIZERS
───────────────────────────────────────────────────────────

🎮 CONTROLS:
  • ◀ Previous: Go to previous step
  • ▶ Play: Auto-play through all steps
  • ⏸ Pause: Pause auto-play
  • Next ▶: Go to next step
  • Slider: Jump to any step directly

📊 WHAT YOU'LL SEE:

Graph Partitioning:
  • Main Graph: Visual representation of the graph
    - Different colors = different partitions
    - Red dashed lines = cut edges (bad!)
    - Gray lines = internal edges (good!)

  • Metrics: Shows how nodes are distributed

  • Explanation: Step-by-step what's happening

Resource Scaling:
  • Server Bars: Show CPU, RAM, and I/O usage
    - Red bars = used resources
    - Light bars = available resources

  • Gauge: Overall system workload level
    - Green = normal (30-80%)
    - Yellow = high (80%+)
    - Orange = low (<30%)

  • Metrics: System statistics

Graph Partitioning:
  ✓ Understand how to divide graphs efficiently
  ✓ Learn about cut edges and their impact
  ✓ See optimization algorithms in action

Resource Scaling:
  ✓ Understand elastic resource management
  ✓ Learn when to scale up vs scale down
  ✓ See load balancing in practice
    """)

    input("\n\nPress Enter to return to main menu...")

def launch_graph_partitioner():
    """Launch the graph partitioning visualizer"""
    clear_screen()
    print("\n" + "="*60)
    print("  📊 GRAPH PARTITIONING VISUALIZER")
    print("="*60 + "\n")

    print("Select a topology:\n")
    print("  1. Simple Topology (9 nodes) - Recommended for beginners")
    print("  2. DEBS Topology (7 nodes) - Real-world example")
    print("  3. Back to main menu")
    print()

    choice = input("Your choice (1-3): ").strip()

    if choice == '3':
        return

    try:
        from beautiful_graph_partitioner import (
            BeautifulGraphPartitioner,
            create_simple_topology,
            create_debs_topology
        )

        if choice == '1':
            print("\nLoading Simple Topology...")
            G = create_simple_topology()
            num_partitions = 3
        elif choice == '2':
            print("\nLoading DEBS Topology...")
            G = create_debs_topology()
            num_partitions = 3
        else:
            print("Invalid choice. Returning to menu...")
            input("\nPress Enter to continue...")
            return

        print(f"   • {len(G.nodes())} nodes, {len(G.edges())} edges")
        print(f"   • {num_partitions} partitions")
        print("\n Launching visualizer...")
        print("\n TIP: Press the ▶ Play button to start the animation!")
        print("        Use ◀ Previous and Next ▶ to step through\n")

        visualizer = BeautifulGraphPartitioner(G, num_partitions)
        visualizer.show()

    except Exception as e:
        print(f"\n Error: {e}")
        import traceback
        traceback.print_exc()
        input("\nPress Enter to continue...")

def launch_resource_scaler():
    """Launch the resource scaling visualizer"""
    clear_screen()
    print("\n" + "="*60)
    print("    RESOURCE SCALING VISUALIZER")
    print("="*60 + "\n")

    print("This visualizer shows how a distributed system")
    print("automatically scales resources based on workload.\n")

    print("You'll see:")
    print("  • Servers scaling up when overloaded")
    print("  • Servers scaling down when underutilized")
    print("  • Real-time resource monitoring")
    print("  • Load balancing in action")

    input("\nPress Enter to launch...")

    try:
        from beautiful_resource_scaler import BeautifulResourceScaler

        print("\n Launching visualizer...")
        print("\n TIP: Press the ▶ Play button to watch the full scenario!")
        print("        Watch how the system responds to workload changes\n")

        scaler = BeautifulResourceScaler(max_servers=8)
        scaler.show()

    except Exception as e:
        print(f"\n Error: {e}")
        import traceback
        traceback.print_exc()
        input("\nPress Enter to continue...")

def main():
    """Main entry point"""
    try:
        while True:
            clear_screen()
            print_banner()
            print_main_menu()

            choice = input("Enter your choice (1-4): ").strip()

            if choice == '1':
                launch_graph_partitioner()
            elif choice == '2':
                launch_resource_scaler()
            elif choice == '3':
                print_tutorial()
            elif choice == '4':
                clear_screen()
                print("\n" + "="*60)
                print("   Thank you for using the Visualization Suite!")
                print("="*60 + "\n")
                sys.exit(0)
            else:
                print("\n Invalid choice. Please enter 1-4.")
                input("\nPress Enter to continue...")

    except KeyboardInterrupt:
        clear_screen()
        print("\n\n Program interrupted by user. Goodbye!\n")
        sys.exit(1)
    except Exception as e:
        print(f"\n Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()