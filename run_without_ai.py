#!/usr/bin/env python3
"""
Run the negotiation script without AI features to avoid quota issues
"""

import subprocess
import sys
import os

def main():
    print("🏥 Running Hospital Price Negotiation System (No AI Mode)")
    print("=" * 60)
    print("ℹ️  This will run the script with AI features disabled to avoid quota issues.")
    print("ℹ️  You'll still get all the standard analysis and negotiation strategies.")
    print("=" * 60)
    
    # Set environment variable to disable AI
    env = os.environ.copy()
    env['DISABLE_AI'] = 'true'
    
    try:
        # Run the streamlit app
        result = subprocess.run([
            sys.executable, '-m', 'streamlit', 'run', 'negotiation.py',
            '--server.port', '8501',
            '--server.headless', 'true'
        ], env=env, cwd=os.getcwd())
        
        if result.returncode == 0:
            print("✅ Application completed successfully")
        else:
            print(f"❌ Application exited with code {result.returncode}")
            
    except KeyboardInterrupt:
        print("\n🛑 Application stopped by user")
    except Exception as e:
        print(f"❌ Error running application: {e}")

if __name__ == "__main__":
    main()
