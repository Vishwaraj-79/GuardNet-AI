import torch
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models.simple_gnn import SimpleGNN
from models.data_generator import networkx_to_pyg
import networkx as nx
import numpy as np

app = FastAPI(title="GuardNet AI Detection API")

# Load model
device = 'cuda' if torch.cuda.is_available() else 'cpu'
print(f"📱 Loading model on {device}...")

model = SimpleGNN(input_dim=8, hidden_dim=128, output_dim=2)
try:
    model.load('models/guardnet_final_model.pth')
    print("✅ Model loaded successfully!")
except:
    print("⚠️ Model not found, using untrained model")
model.to(device)
model.eval()

class FlowData(BaseModel):
    flows: list
    slice_id: str

@app.get("/")
def root():
    return {"message": "GuardNet AI Detection API is running!", "status": "healthy"}

@app.get("/health")
def health():
    return {"status": "healthy", "device": device}

@app.post("/detect")
def detect_attack(data: FlowData):
    try:
        # Build graph from flows with ALL node types
        G = nx.MultiDiGraph()
        
        # Add Slice nodes (required by model)
        slices = ['eMBB', 'URLLC', 'mMTC', 'Control', 'Management']
        for slice_name in slices:
            G.add_node(f"slice_{slice_name}", type='Slice', slice_type=slice_name, features=[0.5]*8)
        
        # Add NF nodes
        nfs = ['AMF', 'SMF', 'UPF', 'NEF', 'NWDAF']
        for nf_type in nfs:
            G.add_node(f"nf_{nf_type}", type='NF', nf_type=nf_type, features=[0.5]*8)
        
        # Add Service nodes
        services = ['VoIP', 'Video', 'Web', 'IoT']
        for service in services:
            G.add_node(f"service_{service}", type='Service', service_type=service, features=[0.5]*8)
        
        # Add UE nodes from flows
        for i, flow in enumerate(data.flows):
            src = flow.get('src_ip', f'unknown_{i}')
            dst = flow.get('dst_ip', f'unknown_{i}')
            
            # Add UE node
            G.add_node(f"ue_{src}", type='UE', features=[0.5]*8)
            G.add_edge(f"ue_{src}", f"nf_{dst}", relation='communicates')
            
            # Connect UE to slice
            slice_name = data.slice_id if data.slice_id in slices else 'eMBB'
            G.add_edge(f"ue_{src}", f"slice_{slice_name}", relation='belongs_to')
        
        # Connect NFs to slice
        slice_name = data.slice_id if data.slice_id in slices else 'eMBB'
        for nf_type in nfs:
            if np.random.random() > 0.4:
                G.add_edge(f"nf_{nf_type}", f"slice_{slice_name}", relation='hosts')
        
        # Connect NFs to Services
        for nf_type in nfs:
            for service in services:
                if np.random.random() > 0.5:
                    G.add_edge(f"nf_{nf_type}", f"service_{service}", relation='provides')
        
        # Convert to PyG
        from torch_geometric.data import Batch
        pyg_data = networkx_to_pyg(G, label=0)
        
        # Wrap as Batch
        pyg_data = Batch.from_data_list([pyg_data])
        
        # Move to device
        for key in pyg_data.x_dict:
            pyg_data.x_dict[key] = pyg_data.x_dict[key].to(device)
        for key in pyg_data.edge_index_dict:
            pyg_data.edge_index_dict[key] = pyg_data.edge_index_dict[key].to(device)
        
        # Predict
        with torch.no_grad():
            output = model(pyg_data.x_dict, pyg_data.edge_index_dict, pyg_data.batch_dict)
            probs = torch.softmax(output, dim=1)
            pred = torch.argmax(output, dim=1)
        
        is_attack = bool(pred.item() == 1)
        confidence = float(probs[0][1].item()) if is_attack else float(probs[0][0].item())
        
        return {
            "is_attack": is_attack,
            "confidence": round(confidence, 4),
            "attack_type": "suspected" if is_attack else "normal",
            "flows_processed": len(data.flows),
            "slice_id": data.slice_id
        }
    
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    print("🚀 Starting GuardNet AI Detection API...")
    uvicorn.run(app, host="0.0.0.0", port=8000)
