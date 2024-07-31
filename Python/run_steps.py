import subprocess

def run_sequential_scripts(folder_name):
    """Runs Python scripts within a specified folder sequentially."""

    import os

    script_files = [f for f in os.listdir(folder_name) if f.endswith('.py')]  
    script_files.sort()  # Ensure correct order (Step1, Step2, etc.)

    for script_file in script_files:
        script_path = os.path.join(folder_name, script_file)
        print(f"Running: {script_file}")
        
        # Run the script using subprocess and wait for it to finish
        subprocess.run(["python", script_path])
        
        print(f"Finished: {script_file}\n")  

if __name__ == "__main__":
    folder_to_run = "Steps"  
    run_sequential_scripts(folder_to_run) 
