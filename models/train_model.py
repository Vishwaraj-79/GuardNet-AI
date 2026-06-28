
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models.simple_gnn import SimpleGNN
from models.trainer import ModelTrainer
from models.data_generator import prepare_datasets
import torch
from datetime import datetime

def main():
    print("🛡️ GuardNet AI - Model Training")
    print("=" * 60)
    
    # Generate data
    train_graphs, train_labels, test_graphs, test_labels = prepare_datasets(
        num_samples=500,  # Increased for better training
        test_size=0.2
    )
    
    # Initialize model
    device = 'cuda' if torch.cuda.is_available() else 'cpu'
    print(f"📱 Using device: {device}")
    
    model = SimpleGNN(
        input_dim=8,
        hidden_dim=128,    # Increased from 64
        output_dim=2
    )
    
    # Train with better parameters
    trainer = ModelTrainer(model, device=device)
    history = trainer.train(
        train_graphs,
        test_graphs,
        epochs=100,        # More epochs
        batch_size=32,     # Larger batch
        patience=20        # More patience
    )
    
    # Evaluate
    metrics = trainer.evaluate(test_graphs)
    
    # Save final model
    model.save('models/guardnet_final_model.pth')
    
    print("\n✅ Training Complete!")
    print(f"📊 Final Metrics: {metrics}")
    
    return model, metrics

if __name__ == "__main__":
    main()
