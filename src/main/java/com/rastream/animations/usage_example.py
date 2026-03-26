"""
Complete usage examples for Graph Partitioning Visualizer - FIXED VERSION
"""

import sys
import os
from graph_partitioning_animator import (
    GraphPartitioningVisualizer,
    create_wordcount_topology,
    create_debs_2024_topology,
    create_custom_topology
)
from resource_scaling_animator import ResourceScalingAnimator

def clear_screen():
    """Clear terminal screen"""
    os.system('cls' if os.name == 'nt' else 'clear')

def print_header(title):
    """Print formatted header"""
    print("\n" + "="*70)
    print(f"  {title}")
    print("="*70 + "\n")

def print_menu():
    """Print main menu"""
    print_header("Graph Partitioning Visualizer - Main Menu")
    print("""
  1.  WordCount Topology (Simple - 4 nodes, 2 partitions)
  2.  DEBS 2024 Topology (Complex - 7 nodes, 3 partitions)
  3.  Custom Random Topology (Medium - 15 nodes, 4 partitions)
  4.  Resource Scaling Animation (Demonstrates scale up/down)
  5.  Large Topology (Advanced - 30 nodes, 6 partitions)
  6.  Exit

""")

def get_valid_choice():
    """Get valid menu choice"""
    while True:
        try:
            choice = input("Select example (1-6): ").strip()
            if choice in ['1', '2', '3', '4', '5', '6']:
                return choice
            else:
                print(" Invalid choice. Please enter 1-6.")
        except EOFError:
            print("\n EOF detected. Exiting...")
            return '6'
        except KeyboardInterrupt:
            print("\n Interrupted by user. Exiting...")
            return '6'

def example_1_wordcount():
    """Example 1: WordCount topology"""
    print_header("Example 1: WordCount Stream Application")
    print("""
This example demonstrates partitioning of a simple WordCount
stream application into 2 subgraphs.

Topology Structure:
  Spout (Source)
    ↓
  Splitter (Tokenizer)
    ↓
  Counter (Aggregator)
    ↓
  Sink (Output)

Features Shown:
  • Graph partitioning using Simulated Annealing
  • Internal vs. cut edges visualization
  • Task distribution across partitions
  • Optimization progress tracking
    """)
    
    print("\n⏳ Loading topology...")
    G = create_wordcount_topology()
    print(f"✓ Loaded: {len(G.nodes())} nodes, {len(G.edges())} edges")
    
    print("\n📊 Creating visualizer...")
    visualizer = GraphPartitioningVisualizer(G, num_partitions=2)
    
    print("\n🎬 Starting animation...")
    print("   • Press 'Play/Pause' to control animation")
    print("   • Use 'Speed' slider to adjust animation speed")
    print("   • Press 'Reset' to restart animation")
    print("   • Close window when done\n")
    
    visualizer.show()

def example_2_debs():
    """Example 2: DEBS 2024 topology"""
    print_header("Example 2: DEBS 2024 Complex Topology")
    print("""
This example demonstrates partitioning of a complex
DEBS 2024 topology into 3 subgraphs.

Topology Structure:
  Data Source
    ↓
  Data Filter → Event Detection → Aggregation → Data Storage
                      ↓
                Anomaly Detection → Alert System ↘
                                        ↓
                                   Data Storage

Features Shown:
  • Complex topology with multiple paths
  • Balancing communication-intensive pairs
  • Resource requirements across partitions
  • Multi-level graph analysis
    """)
    
    print("\n⏳ Loading topology...")
    G = create_debs_2024_topology()
    print(f"✓ Loaded: {len(G.nodes())} nodes, {len(G.edges())} edges")
    
    print("\n📊 Creating visualizer...")
    visualizer = GraphPartitioningVisualizer(G, num_partitions=3)
    
    print("\n🎬 Starting animation...")
    print("   • Watch how the algorithm partitions the graph")
    print("   • Observe edge weight calculations")
    print("   • Monitor fitness score improvements\n")
    
    visualizer.show()

def example_3_custom():
    """Example 3: Custom random topology"""
    print_header("Example 3: Custom Random Topology")
    print("""
This example demonstrates partitioning of a custom
random topology with 15 nodes into 4 partitions.

This topology is randomly generated, so:
  • Each run will have different structure
  • Edge weights vary randomly
  • Partitioning strategy adapts to structure
    """)
    
    print("\n🎲 Generating random topology...")
    G = create_custom_topology(num_nodes=15, num_edges=25)
    print(f"✓ Generated: {len(G.nodes())} nodes, {len(G.edges())} edges")
    
    print("\n📊 Creating visualizer...")
    visualizer = GraphPartitioningVisualizer(G, num_partitions=4)
    
    print("\n🎬 Starting animation...\n")
    
    visualizer.show()

def example_4_resource_scaling():
    """Example 4: Resource scaling"""
    print_header("Example 4: Resource Scaling Animation")
    print("""
This example demonstrates resource scaling operations
showing extend and shrink based on data stream rates.

Scenario Overview:
  Phase 1: System starts OVERLOADED (high stream rate)
  Phase 2: Resources are EXTENDED (more nodes added)
  Phase 3: Stream rate NORMALIZES (balanced state)
  Phase 4: Stream rate DECREASES (underload detected)
  Phase 5: Resources are SHRUNK (unused nodes removed)
  Phase 6: System returns to BALANCED state

What You'll See:
  • CPU, Memory, I/O utilization patterns
  • Node allocation and deallocation
  • Subgraph migration across nodes
  • Resource efficiency metrics
    """)
    
    print("\n📊 Creating resource scaler...")
    animator = ResourceScalingAnimator(num_nodes=10, num_subgraphs=8)
    
    print("✓ Animator created")
    print("\n🎬 Starting animation...")
    print("   • Observe resource allocation changes")
    print("   • Note underload and overload thresholds")
    print("   • Watch automatic scaling operations\n")
    
    animator.show()

def example_5_large():
    """Example 5: Large topology"""
    print_header("Example 5: Large Topology Partitioning")
    print("""
This example demonstrates partitioning of a larger topology
with 30 nodes into 6 subgraphs.

This shows the algorithm's scalability with:
  • 30 compute nodes
  • Complex inter-node dependencies
  • 6 subgraph partitions
  • High edge count for optimization
    """)
    
    print("\n🎲 Generating large random topology...")
    G = create_custom_topology(num_nodes=30, num_edges=50)
    print(f"✓ Generated: {len(G.nodes())} nodes, {len(G.edges())} edges")
    
    print("\n📊 Creating visualizer (this may take a moment)...")
    visualizer = GraphPartitioningVisualizer(G, num_partitions=6)
    
    print("\n🎬 Starting animation...")
    print("   • Watch performance with larger graphs")
    print("   • Observe optimization convergence")
    print("   • Monitor multiple partition balancing\n")
    
    visualizer.show()

def main():
    """Main entry point"""
    try:
        while True:
            print_menu()
            choice = get_valid_choice()
            
            if choice == '1':
                example_1_wordcount()
            elif choice == '2':
                example_2_debs()
            elif choice == '3':
                example_3_custom()
            elif choice == '4':
                example_4_resource_scaling()
            elif choice == '5':
                example_5_large()
            elif choice == '6':
                print_header("Thank You!")
                print("Exiting Graph Partitioning Visualizer...\n")
                sys.exit(0)
                
    except KeyboardInterrupt:
        print("\n\n❌ Program interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
