#!/usr/bin/env python3
"""
Startup script for the Data Quality Management System
"""
import subprocess
import sys
import os
import time
import threading
import argparse
from pathlib import Path

def run_backend():
    """Start the FastAPI backend server"""
    print("Starting FastAPI backend on port 8000...")
    try:
        # Change to project root directory
        os.chdir(Path(__file__).parent)
        
        # Start uvicorn server
        subprocess.run([
            sys.executable, "-m", "uvicorn", 
            "backend.main:app", 
            "--host", "0.0.0.0", 
            "--port", "8000", 
            "--reload"
        ])
    except KeyboardInterrupt:
        print("Backend server stopped.")
    except Exception as e:
        print(f"Error starting backend: {e}")

def run_frontend():
    """Start the React frontend development server"""
    print("Starting React frontend on port 3000...")
    try:
        # Change to frontend directory
        frontend_dir = Path(__file__).parent / "frontend"
        
        if not frontend_dir.exists():
            print("Frontend directory not found. Frontend code may not be available.")
            return
            
        os.chdir(frontend_dir)
        
        # Find npm executable and setup environment
        npm_paths = [
            "npm",
            "C:\\Program Files\\nodejs\\npm.cmd",
            "C:\\Program Files (x86)\\nodejs\\npm.cmd"
        ]
        
        npm_exe = None
        env = os.environ.copy()
        
        for npm_path in npm_paths:
            try:
                subprocess.run([npm_path, "--version"], 
                              capture_output=True, check=True, shell=True)
                npm_exe = npm_path
                # Add Node.js to PATH if using full path
                if "Program Files" in npm_path:
                    nodejs_dir = os.path.dirname(npm_path)
                    if nodejs_dir not in env.get("PATH", ""):
                        env["PATH"] = f"{nodejs_dir};{env.get('PATH', '')}"
                        print(f"✅ Added Node.js to PATH: {nodejs_dir}")
                break
            except (subprocess.CalledProcessError, FileNotFoundError):
                continue
        
        if not npm_exe:
            print("❌ npm not found. Cannot start frontend.")
            return
        
        # Check if node_modules exists
        if not (frontend_dir / "node_modules").exists():
            print("Installing frontend dependencies...")
            subprocess.run([npm_exe, "install"], check=True, shell=True, env=env)
        
        # Start React development server
        print("🚀 Starting React development server...")
        subprocess.run([npm_exe, "start"], shell=True, env=env)
    except KeyboardInterrupt:
        print("Frontend server stopped.")
    except Exception as e:
        print(f"Error starting frontend: {e}")
        print("Frontend is optional. Backend API is still available at http://localhost:8000")

def check_prerequisites():
    """Check if all prerequisites are installed"""
    missing_python = []
    missing_frontend = []
    
    # Check Python version
    if sys.version_info < (3, 8):
        missing_python.append("Python 3.8 or higher")
    
    # Check if Node.js is installed - try multiple methods
    node_available = False
    node_paths = [
        "node",
        "C:\\Program Files\\nodejs\\node.exe",
        "C:\\Program Files (x86)\\nodejs\\node.exe"
    ]
    
    for node_path in node_paths:
        try:
            result = subprocess.run([node_path, "--version"], 
                                  capture_output=True, check=True, shell=True)
            node_available = True
            print(f"✅ Found Node.js at: {node_path}")
            break
        except (subprocess.CalledProcessError, FileNotFoundError):
            continue
    
    if not node_available:
        missing_frontend.append("Node.js")
    
    # Check if npm is installed - try multiple methods
    npm_available = False
    npm_paths = [
        "npm",
        "C:\\Program Files\\nodejs\\npm.cmd",
        "C:\\Program Files (x86)\\nodejs\\npm.cmd"
    ]
    
    for npm_path in npm_paths:
        try:
            result = subprocess.run([npm_path, "--version"], 
                                  capture_output=True, check=True, shell=True)
            npm_available = True
            print(f"✅ Found npm at: {npm_path}")
            break
        except (subprocess.CalledProcessError, FileNotFoundError):
            continue
    
    if not npm_available:
        missing_frontend.append("npm")
    
    # Critical Python requirements
    if missing_python:
        print("❌ Missing critical prerequisites:")
        for req in missing_python:
            print(f"  - {req}")
        print("\nPlease install the missing prerequisites and run again.")
        return False, False
    
    # Frontend requirements (optional)
    if missing_frontend:
        print("⚠️  Frontend requirements not met:")
        for req in missing_frontend:
            print(f"  - {req}")
        print("\n📋 Options:")
        print("  1. Install Node.js from https://nodejs.org/ for full UI experience")
        print("  2. Continue with backend-only mode (API + documentation)")
        
        choice = input("\nWould you like to continue with backend-only mode? (y/n): ").lower()
        if choice in ['y', 'yes']:
            print("✅ Continuing with backend-only mode...")
            return True, False
        else:
            print("Please install Node.js and npm, then run again.")
            return False, False
    
    return True, True

def install_python_dependencies():
    """Install Python dependencies"""
    print("Installing Python dependencies...")
    try:
        subprocess.run([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"], check=True)
        print("Python dependencies installed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error installing Python dependencies: {e}")
        return False
    return True

def main():
    """Main function to start the entire system"""
    parser = argparse.ArgumentParser(description="Start the Data Quality Management System")
    parser.add_argument("--backend-only", action="store_true", 
                       help="Start only the backend server (skip frontend)")
    parser.add_argument("--test-node", action="store_true",
                       help="Test Node.js and npm detection")
    args = parser.parse_args()
    
    # Test Node.js detection
    if args.test_node:
        print("🔍 Testing Node.js and npm detection...")
        print("=" * 50)
        can_run, frontend_available = check_prerequisites()
        if frontend_available:
            print("✅ Both Node.js and npm detected successfully!")
            print("✅ You can run the full system with frontend.")
        elif can_run:
            print("⚠️  Python requirements met, but Node.js/npm not detected.")
            print("ℹ️  You can run with --backend-only mode.")
        else:
            print("❌ Critical requirements missing.")
        return
    
    print("🚀 Starting Data Quality Management System")
    print("=" * 50)
    
    # Check prerequisites
    if args.backend_only:
        print("🔧 Backend-only mode specified")
        can_run, frontend_available = True, False
        # Still check Python version
        if sys.version_info < (3, 8):
            print("❌ Python 3.8 or higher required")
            sys.exit(1)
    else:
        can_run, frontend_available = check_prerequisites()
        if not can_run:
            sys.exit(1)
    
    # Install Python dependencies
    if not install_python_dependencies():
        sys.exit(1)
    
    print("\n📋 System Information:")
    print(f"  - Backend API: http://localhost:8000")
    print(f"  - API Documentation: http://localhost:8000/docs")
    if frontend_available:
        print(f"  - Frontend UI: http://localhost:3000")
    print(f"  - Neo4j Browser: http://localhost:7474")
    print(f"  - Trino Web UI: http://localhost:8080")
    print("\n⚠️  Make sure your local database services are running!")
    print("   - Neo4j should be accessible at http://localhost:7474")
    print("   - Trino should be accessible at http://localhost:8080")
    
    try:
        # Start backend server
        backend_thread = threading.Thread(target=run_backend, daemon=True)
        backend_thread.start()
        time.sleep(3)  # Give backend time to start
        
        if frontend_available:
            # Start frontend server
            frontend_thread = threading.Thread(target=run_frontend, daemon=True)
            frontend_thread.start()
            print("\n✅ Both servers started successfully!")
            print("   - Backend API: http://localhost:8000")
            print("   - Frontend UI: http://localhost:3000")
        else:
            print("\n✅ Backend server started successfully!")
            print("   - Backend API: http://localhost:8000")
            print("   - API Documentation: http://localhost:8000/docs")
            print("   - Test with: python demo_ui_features.py")
        
        print("   Press Ctrl+C to stop all servers")
        
        # Keep main thread alive
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\n🛑 Shutting down servers...")
        print("Goodbye!")

if __name__ == "__main__":
    # Show help if no arguments
    if len(sys.argv) == 1:
        print("🚀 Data Quality Management System")
        print("=" * 50)
        print("Usage:")
        print("  python start_dq_system.py                 # Full system (backend + frontend)")
        print("  python start_dq_system.py --backend-only  # Backend only")
        print("  python start_dq_system.py --test-node     # Test Node.js detection")
        print("  python start_dq_system.py --help          # Show all options")
        print()
        print("Current status:")
        print("  ✅ Backend is ready to run")
        print("  🔍 Frontend requires Node.js/npm")
        print()
        print("For backend-only mode (no Node.js required):")
        print("  python start_dq_system.py --backend-only")
        print()
    
    main() 