pip uninstall -y zillionare-omega
rm dist/*
poetry build
pip install -qq dist/zillionare_omega-1.0.1-py3-none-any.whl
