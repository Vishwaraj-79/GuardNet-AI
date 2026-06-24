import json
import networkx as nx
import numpy as np
from collections import defaultdict
from datetime import datetime, timedelta
from kafka import KafkaConsumer, KafkaProducer
import threading
import time
import pickle

class GraphBuilder:
    def __init__(self, window_size=10):
        """
        window_size: seconds to build graph from
        """
        self.window_size = window_size
        self.flow_buffer = []
        self.graph_queue = []
        self.node_features = {}
        self.ue_list = set()
        self.nf_list = set()
        self.slice_list = set()
        
        # Kafka setup
        self.consumer = KafkaConsumer(
            'featured-flows',
            bootstrap_servers='localhost:9092',
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True
        )
        
        self.producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: pickle.dumps(v)
        )
        
        self.last_window_time = datetime.now()
        self.running = False
        self.graph_counter = 0
    
    def build_heterogeneous_graph(self, features):
        """Build a heterogeneous graph from feature data"""
        G = nx.MultiDiGraph()
        
        # Define node types
        node_types = {
            'UE': [],
            'NF': ['AMF', 'SMF', 'UPF', 'NEF', 'NWDAF'],
            'Slice': ['eMBB', 'URLLC', 'mMTC', 'Control', 'Management'],
            'Service': ['VoIP', 'Video', 'Web', 'IoT', 'Control']
        }
        
        # Add slice nodes
        for slice_name in node_types['Slice']:
            G.add_node(f'slice_{slice_name}', type='Slice', slice_type=slice_name)
            self.slice_list.add(slice_name)
        
        # Add NF nodes
        for nf_type in node_types['NF']:
            G.add_node(f'nf_{nf_type}', type='NF', nf_type=nf_type)
            self.nf_list.add(nf_type)
        
        # Add UE nodes (from features)
        for slice_data in features.values():
            flow_count = slice_data.get('flow_count', 0)
            unique_ues = slice_data.get('unique_ues', 0)
            
            # Generate sample UEs based on unique count
            for i in range(min(unique_ues, 10)):  # Limit to 10 UEs per slice
                ue_id = f'ue_{slice_data["slice_type"]}_{i}'
                G.add_node(ue_id, type='UE', slice=slice_data['slice_type'])
                self.ue_list.add(ue_id)
                
                # Connect UE to its slice
                G.add_edge(ue_id, f'slice_{slice_data["slice_type"]}', 
                          relation='belongs_to', weight=1.0)
                
                # Connect UE to NFs (based on traffic patterns)
                if slice_data.get('protocol_tcp_ratio', 0) > 0.3:
                    G.add_edge(ue_id, 'nf_UPF', relation='communicates', weight=0.6)
                if slice_data.get('protocol_udp_ratio', 0) > 0.3:
                    G.add_edge(ue_id, 'nf_AMF', relation='communicates', weight=0.4)
                
                # Add edge features
                G[ue_id][f'slice_{slice_data["slice_type"]}']['weight'] = 0.8
                G[ue_id][f'slice_{slice_data["slice_type"]}']['packet_rate'] = slice_data.get('packet_rate', 10)
        
        # Connect NFs to Slices
        for slice_name in node_types['Slice']:
            for nf_type in node_types['NF']:
                weight = np.random.uniform(0.3, 0.9)
                G.add_edge(f'nf_{nf_type}', f'slice_{slice_name}', 
                          relation='hosts', weight=weight)
        
        # Add node features
        for node in G.nodes():
            node_data = G.nodes[node]
            node_type = node_data.get('type', 'Unknown')
            
            # Set feature vectors based on node type
            if node_type == 'Slice':
                slice_type = node_data.get('slice_type', '')
                if slice_type in features:
                    f = features[slice_type]
                    # Feature vector for slice node
                    features_vec = np.array([
                        f.get('flow_count', 0) / 100,
                        f.get('packet_rate', 0) / 1000,
                        f.get('byte_rate', 0) / 1000000,
                        f.get('unique_ues', 0) / 50,
                        f.get('avg_qci', 5) / 9,
                        f.get('malicious_count', 0) / 10,
                        f.get('protocol_tcp_ratio', 0.5),
                        f.get('protocol_udp_ratio', 0.3),
                        f.get('protocol_icmp_ratio', 0.1)
                    ])
                    G.nodes[node]['features'] = features_vec.tolist()
            
            elif node_type == 'UE':
                # Simple feature vector for UE
                features_vec = np.array([
                    np.random.uniform(0, 1),  # Activity level
                    np.random.uniform(0, 1),  # Data usage
                    np.random.uniform(0, 1),  # Mobility
                    np.random.uniform(0, 1)   # Security risk
                ])
                G.nodes[node]['features'] = features_vec.tolist()
            
            elif node_type == 'NF':
                # Feature vector for NF
                features_vec = np.array([
                    np.random.uniform(0.3, 0.9),  # Load
                    np.random.uniform(0.1, 0.8),  # Latency
                    np.random.uniform(0.5, 1.0),  # Availability
                    np.random.uniform(0, 0.3)     # Error rate
                ])
                G.nodes[node]['features'] = features_vec.tolist()
        
        # Add edge features (weights already added)
        for u, v, data in G.edges(data=True):
            data['weight'] = data.get('weight', np.random.uniform(0.3, 0.8))
        
        return G
    
    def process_stream(self):
        """Main processing loop"""
        print(f"🔵 Graph Builder started (window={self.window_size}s)")
        print("🔵 Building graphs from featured flows...\n")
        
        self.running = True
        buffer = []
        
        try:
            for message in self.consumer:
                features = message.value
                current_time = datetime.now()
                
                # Add features to buffer
                buffer.append(features)
                
                # Check if window is complete
                if (current_time - self.last_window_time).total_seconds() >= self.window_size:
                    if buffer:
                        # Build graph from accumulated features
                        G = self.build_heterogeneous_graph(features)
                        self.graph_counter += 1
                        
                        # Store graph metadata
                        graph_info = {
                            'graph_id': self.graph_counter,
                            'timestamp': current_time.isoformat(),
                            'nodes': G.number_of_nodes(),
                            'edges': G.number_of_edges(),
                            'graph_data': G
                        }
                        
                        # Send to Kafka
                        self.producer.send('graphs', value=graph_info)
                        
                        # Print summary
                        print(f"📊 Graph #{self.graph_counter}: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
                        print(f"   Slices: {list(self.slice_list)[:3]}...")
                        print(f"   UEs: {list(self.ue_list)[:3]}...")
                        
                        # Store in queue for later use
                        self.graph_queue.append(graph_info)
                        if len(self.graph_queue) > 100:
                            self.graph_queue.pop(0)
                        
                        # Reset buffer
                        buffer = []
                        self.last_window_time = current_time
                    
        except KeyboardInterrupt:
            print("\n🔴 Stopping graph builder...")
        finally:
            self.consumer.close()
            self.producer.close()
            self.running = False
    
    def get_latest_graph(self):
        """Get most recent graph"""
        if self.graph_queue:
            return self.graph_queue[-1]['graph_data']
        return None
    
    def get_graph_count(self):
        """Get number of graphs built"""
        return self.graph_counter
    
    def run(self):
        """Start the graph builder"""
        self.process_stream()

if __name__ == "__main__":
    builder = GraphBuilder(window_size=10)
    builder.run()