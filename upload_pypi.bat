del /q dist\*
python -m build
pause
python -m twine upload dist\*
pause