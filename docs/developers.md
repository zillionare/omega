1. create a virtual env, named as omega (conventional, not necessary)
2. conda activate omega, then pip install poetry
3. poetry install -E doc -E test -E dev
4. edit your .bashrc, set environment variables as the following:
```
JQ_ACCOUNT=
JQ_PASSWORD=
MAIL_FROM=
MAIL_SERVER=
MAIL_TO=
MAIL_PASSWORD=
__cfg4py_server_role__=DEV
```
test will not go without these environment variables.
5. run command `tox`
