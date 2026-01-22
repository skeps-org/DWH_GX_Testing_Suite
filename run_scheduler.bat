@echo off
TITLE GX Data Quality Scheduler
REM ====================================================
REM DWH GX Testing Suite - Python Scheduler
REM Keep this window OPEN to run the daily job
REM ====================================================

cd /d "C:\Users\VineetJha\Downloads\Work - Vineet\DWH GX Testing Suite"
call gxvenv\Scripts\activate.bat

python scheduler.py

pause
