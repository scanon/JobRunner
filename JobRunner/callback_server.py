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
    app.run(host=ip, port=port, debug=False, access_log=True)



@app.route("/", methods=["GET", "POST"])
async def root(request):
    logger.info("Hit the root")
    try:
        data = request.json
        logger.warning(f"{data}")
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
    logger.info(f"JOB FAILED!")
    if 'error' in output:
        output['result'] = output['error']
    return output


def _check_finished(info=None):
    global prov
    in_q = app.config["in_q"]
    try:
        # Flush the queue
        while True:
            logger.info(f"Hit the _check_finished for {info}")
            [mtype, fjob_id, output] = in_q.get(block=False)
            logger.info(f"Got in_q check finished of {[mtype, fjob_id, output]}")

            if mtype == "output":
                if 'error' in output:
                    _job_failed(output)
                logger.info(f"Job output is OK")
                outputs[fjob_id] = output
            elif mtype == "prov":
                prov = output
    except Empty:
        pass


def _check_rpc_token(token):
    logger.info("Hit the rpc token check")
    if token != app.config.get("token"):
        if app.config.get("bypass_token"):
            pass
        else:
            abort(401)
    logger.info("success")


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
    return {"result": [resp]}


async def _process_rpc(data, token):
    """
    Handle KBase SDK App Client Requests
    """

    (module, method) = data["method"].split(".")
    # async submit job
    if method.startswith("_") and method.endswith("_submit"):
        logger.info("Hit the async")
        return _handle_submit(module, method, data, token)
    # check job
    elif method.startswith("_check_job"):
        logger.info("Hit the checkjob")
        return _handle_checkjob(data=data)
    # Provenance
    elif method.startswith("get_provenance"):
        logger.info("Hit the prov")
        return _handle_provenance()
    else:
        # Sync Job
        logger.info("About to generate job id for the sync method for " + data["method"])
        _check_rpc_token(token)
        job_id = str(uuid.uuid1())


        logger.info(data["method"])
        data["method"] = "%s.%s" % (module, method)

        logger.info(f"Hit the sync 2 {job_id} for" + data["method"])
        app.config["out_q"].put(["submit", job_id, data])
        logger.info(f"Hit the sync 3 {job_id} for" + data["method"])
        try:
            while True:
                logger.info(f"Hit the sync loop for {job_id} for " + data["method"])
                _check_finished(f'synk check for data["method"] for {job_id}')
                if job_id in outputs:
                    logger.info("Hit the sync output stage")
                    resp = outputs[job_id]
                    resp["finished"] = 1
                    return resp
                await asyncio.sleep(1)
        except Exception:
            return {"error": "Timeout"}
