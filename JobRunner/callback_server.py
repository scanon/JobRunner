import asyncio
import uuid
from queue import Empty

from sanic import Sanic
from sanic.exceptions import abort, add_status_code
from sanic.response import json
from sanic.log import logger

app = Sanic()
outputs = dict()
prov = None


def start_callback_server(ip, port, out_queue, in_queue, token, bypass_token):
    conf = {
        "token": token,
        "out_q": out_queue,
        "in_q": in_queue,
        "bypass_token": bypass_token,
    }
    app.config.update(conf)
    app.run(host=ip, port=port, debug=False, access_log=False)


@app.route("/", methods=["GET", "POST"])
async def root(request):
    try:
        data = request.json
        if request.method == "POST" and data is not None and "method" in data:
            token = request.headers.get("Authorization")
            return json(await _process_rpc(data, token))
    except Exception as e:
        logger.error("A bad response:")
        logger.error(f"{e}")
        raise e
    return json({})


@add_status_code(500)
def _job_failed(output):
    if 'error' in output:
        output['result'] = output['error']
    return output


def _check_finished():
    global prov
    in_q = app.config["in_q"]
    try:
        # Flush the queue
        while True:
            [mtype, fjob_id, output] = in_q.get(block=False)
            if mtype == "output":
                if 'error' in output:
                    _job_failed(output)

                outputs[fjob_id] = output
            elif mtype == "prov":
                prov = output
    except Empty:
        pass


def _check_rpc_token(token):
    if token != app.config.get("token"):
        if app.config.get("bypass_token"):
            pass
        else:
            abort(401)


def _handle_provenance():
    _check_finished()
    return {"result": [prov]}


def _handle_submit(module, method, data, token):
    _check_rpc_token(token)
    job_id = str(uuid.uuid1())
    data["method"] = "%s.%s" % (module, method[1:-7])
    app.config["out_q"].put(["submit", job_id, data])
    return {"result": [job_id]}


def _handle_checkjob(data):
    if "params" not in data:
        abort(404)
    job_id = data["params"][0]
    _check_finished()
    resp = {"finished": 0}
    if job_id in outputs:
        resp = outputs[job_id]
        resp["finished"] = 1
    return {"result": [resp]}


async def _process_rpc(data, token):
    """
    Handle KBase SDK App Client Requests
    """

    (module, method) = data["method"].split(".")
    # async submit job
    if method.startswith("_") and method.endswith("_submit"):
        return _handle_submit(module, method, data, token)
    # check job
    elif method.startswith("_check_job"):
        return _handle_checkjob(data=data)
    # Provenance
    elif method.startswith("get_provenance"):
        return _handle_provenance()
    else:
        # Sync Job
        _check_rpc_token(token)
        job_id = str(uuid.uuid1())


        logger.info(data["method"])
        data["method"] = "%s.%s" % (module, method)


        app.config["out_q"].put(["submit", job_id, data])
        try:
            while True:
                _check_finished()
                if job_id in outputs:
                    resp = outputs[job_id]
                    resp["finished"] = 1
                    return resp
                await asyncio.sleep(1)
        except Exception:
            return {"error": "Timeout"}
