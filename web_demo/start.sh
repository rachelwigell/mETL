export FLASK_APP=app.py
pushd ./static && yarn build && popd
flask run
