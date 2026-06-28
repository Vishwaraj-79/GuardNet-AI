import torch
import torch.nn as nn
import torch.nn.functional as F
from torch_geometric.nn import SAGEConv, GCNConv, global_mean_pool

class SimpleGNN(nn.Module):
    """
    Simplified GNN for testing - uses SAGEConv instead of HGT
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
    
    def forward(self, x_dict, edge_index_dict):
        # Combine all node features
        all_x = torch.cat([x for x in x_dict.values()], dim=0)
        
        # Combine all edges (simplified - use first edge type)
        edge_index = None
        for edge_type, ei in edge_index_dict.items():
            if ei.size(1) > 0:
                if edge_index is None:
                    edge_index = ei
                else:
                    edge_index = torch.cat([edge_index, ei], dim=1)
        
        if edge_index is None:
            edge_index = torch.empty((2, 0), dtype=torch.long)
        
        # Forward pass
        x = self.conv1(all_x, edge_index)
        x = F.relu(x)
        x = self.conv2(x, edge_index)
        x = F.relu(x)
        x = self.conv3(x, edge_index)
        
        # Global pooling
        x = global_mean_pool(x, batch=torch.zeros(x.size(0), dtype=torch.long))
        
        return self.classifier(x)