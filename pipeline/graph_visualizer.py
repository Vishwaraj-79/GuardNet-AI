import networkx as nx
import matplotlib.pyplot as plt
from datetime import datetime
from kafka import KafkaConsumer
import pickle

class GraphVisualizer:
    def __init__(self):
        self.consumer = KafkaConsumer(
            'graphs',
            bootstrap_servers='localhost:9092',
            value_deserializer=lambda v: pickle.loads(v),
            auto_offset_reset='latest'
        )
        self.graph_count = 0
    
    def visualize_latest_graph(self):
        """Visualize the latest graph"""
        try:
            for message in self.consumer:
                graph_info = message.value
                G = graph_info['graph_data']
                
                self.graph_count += 1
                
                # Create visualization
                plt.figure(figsize=(12, 8))
                
                # Color nodes by type
                color_map = {
                    'UE': 'lightblue',
                    'NF': 'lightgreen',
                    'Slice': 'orange',
                    'Service': 'pink'
                }
                
                node_colors = []
                for node in G.nodes():
                    node_type = G.nodes[node].get('type', 'Unknown')
                    node_colors.append(color_map.get(node_type, 'gray'))
                
                # Draw graph
                pos = nx.spring_layout(G, k=2, iterations=50)
                nx.draw(G, pos, 
                        node_color=node_colors,
                        node_size=500,
                        with_labels=False,
                        edge_color='gray',
                        alpha=0.8)
                
                # Add title
                plt.title(f'5G Network Graph #{self.graph_count}\n'
                         f'{G.number_of_nodes()} nodes, {G.number_of_edges()} edges\n'
                         f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
                
                # Add legend
                from matplotlib.patches import Patch
                legend_elements = [
                    Patch(facecolor='lightblue', label='UE'),
                    Patch(facecolor='lightgreen', label='NF'),
                    Patch(facecolor='orange', label='Slice'),
                    Patch(facecolor='pink', label='Service')
                ]
                plt.legend(handles=legend_elements, loc='upper right')
                
                plt.tight_layout()
                plt.savefig(f'graphs/graph_{self.graph_count}.png')
                print(f"✅ Graph #{self.graph_count} saved to graphs/graph_{self.graph_count}.png")
                
        except KeyboardInterrupt:
            print("\n🔴 Stopping visualizer...")

if __name__ == "__main__":
    import os
    os.makedirs('graphs', exist_ok=True)
    visualizer = GraphVisualizer()
    visualizer.visualize_latest_graph()