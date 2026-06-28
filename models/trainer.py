
import torch
import torch.nn as nn
import torch.optim as optim
from torch_geometric.loader import DataLoader
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
import numpy as np
from datetime import datetime

class ModelTrainer:
    def __init__(self, model, device='cpu'):
        self.model = model.to(device)
        self.device = device
        self.optimizer = optim.Adam(model.parameters(), lr=0.001)
        self.criterion = nn.CrossEntropyLoss()
        
        self.history = {
            'train_loss': [],
            'val_loss': [],
            'train_acc': [],
            'val_acc': []
        }
    
    def train_epoch(self, train_loader):
        self.model.train()
        total_loss = 0
        correct = 0
        total = 0
        
        for batch in train_loader:
            batch = batch.to(self.device)
            
            self.optimizer.zero_grad()
            # Use batch_dict for heterogeneous graphs
            output = self.model(batch.x_dict, batch.edge_index_dict, batch.batch_dict)
            loss = self.criterion(output, batch.y)
            
            loss.backward()
            self.optimizer.step()
            
            total_loss += loss.item()
            _, predicted = torch.max(output.data, 1)
            total += batch.y.size(0)
            correct += (predicted == batch.y).sum().item()
        
        return total_loss / len(train_loader), correct / total
    
    def validate(self, val_loader):
        self.model.eval()
        total_loss = 0
        correct = 0
        total = 0
        
        with torch.no_grad():
            for batch in val_loader:
                batch = batch.to(self.device)
                
                output = self.model(batch.x_dict, batch.edge_index_dict, batch.batch_dict)
                loss = self.criterion(output, batch.y)
                
                total_loss += loss.item()
                _, predicted = torch.max(output.data, 1)
                total += batch.y.size(0)
                correct += (predicted == batch.y).sum().item()
        
        return total_loss / len(val_loader), correct / total
    
    def train(self, train_data, val_data, epochs=50, batch_size=16, patience=10):
        train_loader = DataLoader(train_data, batch_size=batch_size, shuffle=True)
        val_loader = DataLoader(val_data, batch_size=batch_size, shuffle=False)
        
        best_val_acc = 0
        patience_counter = 0
        
        print(f"🚀 Training started at {datetime.now().strftime('%H:%M:%S')}")
        print(f"📊 Training samples: {len(train_data)}, Validation samples: {len(val_data)}")
        print("=" * 60)
        
        for epoch in range(epochs):
            train_loss, train_acc = self.train_epoch(train_loader)
            val_loss, val_acc = self.validate(val_loader)
            
            self.history['train_loss'].append(train_loss)
            self.history['val_loss'].append(val_loss)
            self.history['train_acc'].append(train_acc)
            self.history['val_acc'].append(val_acc)
            
            if (epoch + 1) % 5 == 0:
                print(f"Epoch {epoch+1:3d}/{epochs} | "
                      f"Train Loss: {train_loss:.4f} | Train Acc: {train_acc:.4f} | "
                      f"Val Loss: {val_loss:.4f} | Val Acc: {val_acc:.4f}")
            
            if val_acc > best_val_acc:
                best_val_acc = val_acc
                patience_counter = 0
                self.model.save('models/best_model.pth')
            else:
                patience_counter += 1
                if patience_counter >= patience:
                    print(f"⏹️ Early stopping at epoch {epoch+1}")
                    break
        
        print("=" * 60)
        print(f"✅ Training complete! Best Val Acc: {best_val_acc:.4f}")
        return self.history
    
    def evaluate(self, test_data):
        test_loader = DataLoader(test_data, batch_size=16, shuffle=False)
        
        self.model.eval()
        all_preds = []
        all_labels = []
        
        with torch.no_grad():
            for batch in test_loader:
                batch = batch.to(self.device)
                output = self.model(batch.x_dict, batch.edge_index_dict, batch.batch_dict)
                _, predicted = torch.max(output.data, 1)
                all_preds.extend(predicted.cpu().numpy())
                all_labels.extend(batch.y.cpu().numpy())
        
        metrics = {
            'accuracy': accuracy_score(all_labels, all_preds),
            'precision': precision_score(all_labels, all_preds, average='weighted'),
            'recall': recall_score(all_labels, all_preds, average='weighted'),
            'f1': f1_score(all_labels, all_preds, average='weighted')
        }
        
        print("\n📊 Evaluation Results:")
        print(f"   Accuracy:  {metrics['accuracy']:.4f}")
        print(f"   Precision: {metrics['precision']:.4f}")
        print(f"   Recall:    {metrics['recall']:.4f}")
        print(f"   F1-Score:  {metrics['f1']:.4f}")
        
        return metrics
