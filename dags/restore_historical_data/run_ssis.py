import subprocess
from datetime import datetime

# run power shell dtexec
run_dtexec_script = r"dtexec /File csv_to_postgres_ssis\csv_to_postgres_ssis\restore_data_football_raw_db.dtsx"

run_detexec_SSIS = subprocess.run(run_dtexec_script, capture_output=True, text=True)

stdout = run_detexec_SSIS.stdout

# create SSIS output
now = datetime.now()
dt_string = now.strftime("%Y%m%d_%H%M%S")
ssis_logs_path = f"csv_to_postgres_ssis/logs/ssis_output_{dt_string}.txt"

with open(f"csv_to_postgres_ssis/logs/ssis_output_{dt_string}.txt", 'w') as file:
    file.write(stdout)

# open SSIS output
output_script = f"notepad csv_to_postgres_ssis/logs/ssis_output_{dt_string}.txt"
subprocess.run(output_script)
