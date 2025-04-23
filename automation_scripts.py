import os
import sys
import subprocess
import threading
import time

def find_git():
    """Find Git executable in default Windows locations"""
    possible_paths = [
        r"C:\Program Files\Git\cmd\git.exe",
        r"C:\Program Files (x86)\Git\cmd\git.exe",
        os.path.expanduser("~\\AppData\\Local\\Programs\\Git\\cmd\\git.exe")
    ]
    
    for path in possible_paths:
        if os.path.exists(path):
            return path
    return None

def clone_repository():
    repo_url = "https://github.com/sagaramazonpay/automation_scripts.git"
    base_path = os.path.join(os.getcwd(), "automation_scripts")
    
    try:
        # Find Git executable
        git_path = find_git()
        if not git_path:
            print("Git not found. Please install Git or add it to PATH.")
            sys.exit(1)
            
        print(f"Found Git at: {git_path}")
        
        # Remove existing directory if exists
        if os.path.exists(base_path):
            current_dir = os.getcwd()
            os.chdir('..')
            subprocess.run(['rmdir', '/S', '/Q', base_path], shell=True)
            os.chdir(current_dir)
            
        print(f"Cloning repository to: {base_path}")
        subprocess.run([git_path, 'clone', repo_url], check=True)
        return base_path
        
    except subprocess.CalledProcessError as e:
        print(f"Error with git command: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)


def check_requirements(repo_path):
    print("Checking requirements...")
    requirements_file = os.path.join(repo_path, 'requirements.txt')
    try:
        subprocess.run([sys.executable, '-m', 'pip', 'install', '-r', requirements_file], check=True)
        print("All requirements satisfied!")
    except subprocess.CalledProcessError as e:
        print(f"Warning: Some requirements might not be installed. Continuing anyway...")

def get_user_input():
    global user_choice
    try:
        user_choice = input("\nEnter the number of the script you want to run: ")
    except:
        user_choice = None

def list_scripts(repo_path):
    scripts_dir = os.path.join(repo_path, 'scripts')
    scripts = [f for f in os.listdir(scripts_dir) if f.endswith('.py')]
    
    if not scripts:
        print("No Python scripts found in the scripts directory.")
        sys.exit(1)
    
    print("\nAvailable scripts:")
    for i, script in enumerate(scripts, 1):
        print(f"{i}. {script}")
    
    global user_choice
    user_choice = None
    
    # Start input thread
    input_thread = threading.Thread(target=get_user_input)
    input_thread.daemon = True
    input_thread.start()
    
    # Wait for 10 seconds
    input_thread.join(timeout=10)
    
    # If no input received, default to daily monitoring script
    if user_choice is None:
        print("\nNo input received in 10 seconds. Running daily monitoring script by default...")
        return os.path.join(scripts_dir, 'daily_monitoring_script.py')
    
    try:
        choice = int(user_choice)
        if 1 <= choice <= len(scripts):
            return os.path.join(scripts_dir, scripts[choice - 1])
        else:
            print("Invalid choice. Running daily monitoring script by default...")
            return os.path.join(scripts_dir, 'daily_monitoring_script.py')
    except ValueError:
        print("Invalid input. Running daily monitoring script by default...")
        return os.path.join(scripts_dir, 'daily_monitoring_script.py')

def run_script(script_path):
    print(f"\nRunning {os.path.basename(script_path)}...")
    try:
        subprocess.run([sys.executable, script_path], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running script: {e}")

def main():
    try:
        repo_path = clone_repository()
        check_requirements(repo_path)
        selected_script = list_scripts(repo_path)
        run_script(selected_script)
    finally:
        print("\nExecution completed.")

if __name__ == "__main__":
    main()
