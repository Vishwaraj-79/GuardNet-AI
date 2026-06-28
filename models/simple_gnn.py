
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch_geometric.nn import SAGEConv, global_mean_pool

class SimpleGNN(nn.Module):
    """
    Simplified GNN using SAGEConv - works with heterogeneous data
    """
    def __init__(self, input_dim=8, hidden_dim=64, output_dim=2):
        super().__init__()
        
        self.conv1 = SAGEConv(input_dim, hidden_dim)
        self.conv2 = SAGEConv(hidden_dim, hidden_dim)
        self.conv3 = SAGEConv(hidden_dim, hidden_dim)
        
        self.classifier = nn.Sequential(
            nn.Linear(hidden_dim, 32),
            nn.ReLU(),
            nn.Dropout(0.3),
            nn.Linear(32, output_dim)
        )
    
    def forward(self, x_dict, edge_index_dict, batch_dict=None):
        # Combine all node features
        all_x = torch.cat([x for x in x_dict.values()], dim=0)
        
        # Combine all edges
        all_edges = []
        for edge_type, ei in edge_index_dict.items():
            if ei.size(1) > 0:
                all_edges.append(ei)
        
        if all_edges:
            edge_index = torch.cat(all_edges, dim=1)
        else:
            edge_index = torch.empty((2, 0), dtype=torch.long)
        
        # Create combined batch tensor
        if batch_dict is not None and len(batch_dict) > 0:
            # Get all batch tensors and concatenate them
            batch_list = []
            for node_type, batch_tensor in batch_dict.items():
                # batch_tensor indicates which graph each node belongs to
                batch_list.append(batch_tensor)
            batch = torch.cat(batch_list, dim=0)
        else:
            batch = torch.zeros(all_x.size(0), dtype=torch.long, device=all_x.device)
        
        # Forward pass
        x = self.conv1(all_x, edge_index)
        x = F.relu(x)
        x = self.conv2(x, edge_index)
        x = F.relu(x)
        x = self.conv3(x, edge_index)
        
        # Global pooling
        x = global_mean_pool(x, batch)
        
        return self.classifier(x)
    
    def save(self, path):
        torch.save(self.state_dict(), path)
        print(f"✅ Model saved to {path}")
    
    def load(self, path):
        self.load_state_dict(torch.load(path))
        print(f"✅ Model loaded from {path}")
        return self
