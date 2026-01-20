# Run benchmark with Java configured
$env:JAVA_HOME = 'C:\Java\jdk-11.0.22+7'
$env:Path = 'C:\Java\jdk-11.0.22+7\bin;' + $env:Path

# Activate venv and run
.\.venv\Scripts\python.exe benchmark_simple.py
