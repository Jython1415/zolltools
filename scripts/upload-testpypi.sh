python -m twine upload -r testpypi dist/*
echo "Install with: python -m pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple zolltools==[version]"
