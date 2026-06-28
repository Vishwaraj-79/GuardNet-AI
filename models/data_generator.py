
import torch
from torch_geometric.data import HeteroData
import networkx as nx
import numpy as np
import random
from sklearn.model_selection import train_test_split

def networkx_to_pyg(G, label=0):
    data = HeteroData()
    
    node_type_to_nodes = {}
    for node in G.nodes():
        node_type = G.nodes[node].get('type', 'Unknown')
        if node_type not in node_type_to_nodes:
            node_type_to_nodes[node_type] = []
        node_type_to_nodes[node_type].append(node)
    
    for node_type, nodes in node_type_to_nodes.items():
        if node_type in ['UE', 'NF', 'Slice', 'Service']:
            features = []
            for n in nodes:
                if 'features' in G.nodes[n]:
                    features.append(G.nodes[n]['features'])
                else:
                    features.append([0.0] * 8)
            data[node_type].x = torch.tensor(features, dtype=torch.float)
    
    all_nodes = list(G.nodes())
    node_to_idx = {node: idx for idx, node in enumerate(all_nodes)}
    
    edge_type_to_edges = {
        ('UE', 'communicates', 'NF'): [],
        ('UE', 'belongs_to', 'Slice'): [],
        ('NF', 'hosts', 'Slice'): [],
        ('NF', 'provides', 'Service'): []
    }
    
    for u, v, data_dict in G.edges(data=True):
        if u not in node_to_idx or v not in node_to_idx:
            continue
        u_type = G.nodes[u].get('type', 'Unknown')
        v_type = G.nodes[v].get('type', 'Unknown')
        
        if u_type == 'UE' and v_type == 'NF':
            edge_type = ('UE', 'communicates', 'NF')
        elif u_type == 'UE' and v_type == 'Slice':
            edge_type = ('UE', 'belongs_to', 'Slice')
        elif u_type == 'NF' and v_type == 'Slice':
            edge_type = ('NF', 'hosts', 'Slice')
        elif u_type == 'NF' and v_type == 'Service':
            edge_type = ('NF', 'provides', 'Service')
        else:
            continue
        
        if edge_type in edge_type_to_edges:
            u_idx = node_to_idx[u]
            v_idx = node_to_idx[v]
            if u_idx < len(all_nodes) and v_idx < len(all_nodes):
                edge_type_to_edges[edge_type].append([u_idx, v_idx])
    
    for edge_type, edges in edge_type_to_edges.items():
        if edges:
            edge_index = torch.tensor(edges, dtype=torch.long).t().contiguous()
            data[edge_type].edge_index = edge_index
        else:
            data[edge_type].edge_index = torch.empty((2, 0), dtype=torch.long)
    
    for edge_type in [('UE', 'communicates', 'NF'), ('UE', 'belongs_to', 'Slice'), 
                      ('NF', 'hosts', 'Slice'), ('NF', 'provides', 'Service')]:
        if edge_type not in data:
            data[edge_type].edge_index = torch.empty((2, 0), dtype=torch.long)
    
    data.y = torch.tensor([label], dtype=torch.long)
    return data

def generate_synthetic_graphs(num_samples=500, attack_ratio=0.3):
    graphs = []
    labels = []
    
    slices = ['eMBB', 'URLLC', 'mMTC', 'Control', 'Management']
    nfs = ['AMF', 'SMF', 'UPF', 'NEF', 'NWDAF']
    services = ['VoIP', 'Video', 'Web', 'IoT']
    
    for i in range(num_samples):
        G = nx.MultiDiGraph()
        
        # Add nodes with meaningful features
        for slice_name in slices:
            # Features: [avg_flow_rate, avg_packet_size, qci, load, ...]
            features = [
                random.uniform(0.5, 1.0),  # activity
                random.uniform(0.3, 0.9),  # reliability
                random.uniform(0.1, 0.8),  # latency
                random.uniform(0.2, 0.7),  # throughput
                random.uniform(0.3, 0.9),  # availability
                random.uniform(0.1, 0.6),  # security
                random.uniform(0.4, 0.8),  # qos
                random.uniform(0.2, 0.7)   # load
            ]
            G.add_node(f'slice_{slice_name}', type='Slice', features=features)
        
        for nf_type in nfs:
            features = [
                random.uniform(0.3, 0.9),  # cpu_load
                random.uniform(0.2, 0.8),  # memory
                random.uniform(0.5, 1.0),  # availability
                random.uniform(0.1, 0.4),  # error_rate
                random.uniform(0.3, 0.9),  # throughput
                random.uniform(0.2, 0.7),  # latency
                random.uniform(0.4, 0.8),  # connections
                random.uniform(0.2, 0.6)   # security
            ]
            G.add_node(f'nf_{nf_type}', type='NF', features=features)
        
        for service in services:
            features = [
                random.uniform(0.2, 0.8),
                random.uniform(0.3, 0.7),
                random.uniform(0.4, 0.9),
                random.uniform(0.1, 0.5),
                random.uniform(0.3, 0.8),
                random.uniform(0.2, 0.6),
                random.uniform(0.4, 0.9),
                random.uniform(0.3, 0.7)
            ]
            G.add_node(f'service_{service}', type='Service', features=features)
        
        # Add UE nodes with meaningful features
        num_ues = random.randint(5, 20)
        is_attack = random.random() < attack_ratio
        target_nf = random.choice(nfs) if is_attack else None
        
        for j in range(num_ues):
            slice_name = random.choice(slices)
            
            # Normal UEs have balanced features
            features = [
                random.uniform(0.3, 0.7),
                random.uniform(0.3, 0.7),
                random.uniform(0.3, 0.7),
                random.uniform(0.3, 0.7),
                random.uniform(0.3, 0.7),
                random.uniform(0.3, 0.7),
                random.uniform(0.3, 0.7),
                random.uniform(0.3, 0.7)
            ]
            
            # Attack UEs have spikes in some features
            if is_attack and j < num_ues // 3:
                features = [
                    random.uniform(0.8, 1.0),  # high activity
                    random.uniform(0.8, 1.0),  # high traffic
                    random.uniform(0.7, 1.0),  # high connections
                    random.uniform(0.8, 1.0),  # high throughput
                    random.uniform(0.1, 0.3),  # low availability
                    random.uniform(0.8, 1.0),  # high security alerts
                    random.uniform(0.7, 1.0),  # high qos
                    random.uniform(0.8, 1.0)   # high load
                ]
            
            node = f'ue_{j}_{i}'
            G.add_node(node, type='UE', features=features)
            G.add_edge(node, f'slice_{slice_name}', relation='belongs_to')
            
            # Connect to random NF
            if random.random() > 0.2:
                nf = random.choice(nfs)
                G.add_edge(node, f'nf_{nf}', relation='communicates')
        
        # Connect NFs to slices
        for nf_node in [f'nf_{nf}' for nf in nfs]:
            for slice_name in slices:
                if random.random() > 0.4:
                    G.add_edge(nf_node, f'slice_{slice_name}', relation='hosts')
        
        # Connect NFs to Services
        for nf_node in [f'nf_{nf}' for nf in nfs]:
            for service in services:
                if random.random() > 0.5:
                    G.add_edge(nf_node, f'service_{service}', relation='provides')
        
        pyg_data = networkx_to_pyg(G, label=1 if is_attack else 0)
        graphs.append(pyg_data)
        labels.append(1 if is_attack else 0)
    
    return graphs, labels

def prepare_datasets(num_samples=500, test_size=0.2):
    print(f"📊 Generating {num_samples} synthetic graphs...")
    graphs, labels = generate_synthetic_graphs(num_samples)
    
    train_graphs, test_graphs, train_labels, test_labels = train_test_split(
        graphs, labels,
        test_size=test_size,
        random_state=42,
        stratify=labels
    )
    
    print(f"✅ Training samples: {len(train_graphs)}")
    print(f"✅ Test samples: {len(test_graphs)}")
    
    return train_graphs, train_labels, test_graphs, test_labels
