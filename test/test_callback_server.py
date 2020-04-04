# Import the Sanic app, usually created with Sanic(__name__)
from JobRunner.callback_server import app
import json
from queue import Queue
from unittest.mock import patch

_TOKEN = "bogus"


def _post(data):
    header = {"Authorization": _TOKEN}

    sa = {"access_log": False}
    return app.test_client.post("/", server_kwargs=sa, headers=header, data=data)[1]


def test_index_returns_200():
    response = app.test_client.get("/")[1]
    assert response.status == 200


def test_index_post_empty():
    response = _post(None)
    print(response.json)
    assert response.json == {}


def test_index_post():
    out_q = Queue()
    in_q = Queue()
    conf = {"token": _TOKEN, "out_q": out_q, "in_q": in_q}
    app.config.update(conf)
    data = json.dumps({"method": "bogus._test_submit"})
    response = _post(data)
    assert "result" in response.json
    job_id = response.json["result"]
    mess = out_q.get()
    assert "submit" in mess
    data = json.dumps({"method": "bogus._check_job", "params": [job_id]})
    response = _post(data)
    assert "result" in response.json
    assert response.json["result"][0]["finished"] is False
    data = json.dumps({"method": "bogus.get_provenance", "params": [job_id]})
    response = _post(data)
    assert "result" in response.json
    assert response.json["result"][0] is None
    in_q.put(["prov", job_id, "bogus"])
    response = _post(data)
    assert "result" in response.json
    assert response.json["result"][0] == "bogus"
    in_q.put(["output", job_id, {"foo": "bar"}])
    data = json.dumps({"method": "bogus._check_job", "params": [job_id]})
    response = _post(data)
    assert "result" in response.json
    assert response.json["result"][0]["finished"] is True
    assert "foo" in response.json["result"][0]


@patch("JobRunner.callback_server.uuid", autospec=True)
def test_index_submit_sync(mock_uuid):
    out_q = Queue()
    in_q = Queue()
    conf = {"token": _TOKEN, "out_q": out_q, "in_q": in_q}
    app.config.update(conf)
    mock_uuid.uuid1.return_value = "bogus"
    data = json.dumps({"method": "bogus.test"})
    in_q.put(["output", "bogus", {"foo": "bar"}])
    response = _post(data)
    assert "finished" in response.json
    assert "foo" in response.json
