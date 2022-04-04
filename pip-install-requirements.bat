::ensures that batch file executes in working directory
cd %~dp0

pip install -r requirements.txt

::pause for user to review installation
pause