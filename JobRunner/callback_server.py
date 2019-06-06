from sanic import Sanic
from sanic.response import json
import uuid
from threading import Thread
from multiprocessing import Queue
from queue import Empty
from .provenance import Provenance

app = Sanic()
outputs = dict()

def _check_finished():
    fin_q = app.config['fin_queue']
    try:
        while True:
            [fjob_id, output] = fin_q.get(block=False)
            outputs[fjob_id] = output
    except Empty:
        pass

def _process_rpc(data):
    method = data['method'].split('.')[1]
    # async submi job
    if method.startswith('_') and method.endswith('_submit'):
        action = {'method': 'TODO'}
        app.config['prov'].add_subaction(action)
        job_id = str(uuid.uuid1())
        app.config['queue'].put([job_id, data])
        return {'result': job_id}
    # check job
    elif method.startswith('_check_job'):
        job_id = data['params'][0]
        _check_finished()
        resp = {'finished': False}
        if job_id in outputs:
            resp = outputs[job_id]
            resp['finished'] = True
            print("Subjob %s finished" % (job_id))
        return {'result': [resp]}
    # Provenance
    elif method.startswith('get_provenance'):
        prov = app.config['prov'].get_prov()
        print(prov)
        return {'result':[None]}
    # TODO: this would be a sync job
    else:
        return {'error': 'Unrecongnized post'}


@app.route("/", methods=['GET', 'POST'])
async def root(request):
        data = request.json
        if 'method' in data:
            return json(_process_rpc(data))
        return json({})


def start_callback_server(ip, port, run_queue, fin_queue):
    conf = {
        'queue': run_queue,
        'fin_queue': fin_queue,
        'prov': Provenance()
    }
    app.config.update(conf)
    app.run(host=ip, port=port, debug=False, access_log=False)
        

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
