import asyncio
import uuid
from queue import Empty

from sanic import Sanic
from sanic.config import Config
from sanic.exceptions import abort
from sanic.log import logger
from sanic.response import json

Config.SANIC_REQUEST_TIMEOUT = 300

app = Sanic()
outputs = dict()
prov = None


def start_callback_server(ip, port, out_queue, in_queue, token, bypass_token):
    timeout = 3600
    max_size_bytes = 5000000000
    conf = {
        "token": token,
        "out_q": out_queue,
        "in_q": in_queue,
        "bypass_token": bypass_token,
        "RESPONSE_TIMEOUT": timeout,
        "REQUEST_TIMEOUT": timeout,
        "KEEP_ALIVE_TIMEOUT": timeout,
        "REQUEST_MAX_SIZE" : max_size_bytes
    }
    app.config.update(conf)
    app.run(host=ip, port=port, debug=False, access_log=False)


@app.route("/", methods=["GET", "POST"])
async def root(request):
    try:
        data = request.json
        if request.method == "POST" and data is not None and "method" in data:
            token = request.headers.get("Authorization")
            response = await _process_rpc(data, token)
            return json(response)
    except Exception as e:
        raise e
    return json([{}])


def _check_finished(info=None):
    global prov
    logger.debug(info)
    in_q = app.config["in_q"]
    try:
        # Flush the queue
        while True:
            [mtype, fjob_id, output] = in_q.get(block=False)
            if mtype == "output":
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
    _check_finished(info="Handle Provenance")
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
    _check_finished(f"Checkjob for {job_id}")
    resp = {"finished": 0}
    if job_id in outputs:
        resp = outputs[job_id]
        resp["finished"] = 1
        # resp["result"] = outputs[job_id]

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
        data["method"] = "%s.%s" % (module, method)
        app.config["out_q"].put(["submit", job_id, data])
        try:
            while True:
                _check_finished(f'synk check for {data["method"]} for {job_id}')
                if job_id in outputs:
                    resp = outputs[job_id]
                    resp["finished"] = 1
                    return resp
                await asyncio.sleep(1)
        except Exception as e:
            # Attempt to log error, but this is not very effective..
            exception_message = f"Timeout or exception: {e} {type(e)}"
            logger.error(exception_message)
            error_obj = {
                "error": exception_message,
                "code": "123",
                "message": exception_message,
            }
            outputs[job_id] = {
                "result": exception_message,
                "error": error_obj,
                "finished": 1,
            }
            return outputs[job_id]
