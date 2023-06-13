python -m venv installation_test
source installation_test/bin/activate
pip install --upgrade pip
pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple zolltools
pip uninstall zolltools
deactivate
rm -r installation_test