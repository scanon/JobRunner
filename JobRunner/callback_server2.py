from sanic import Sanic
from sanic.response import json
import os
import uuid
import json
import socket
import asyncio
import sys

app = Sanic()
subactions = []
mr = None
callback_url = ''
config = None
pid = None

async def notify_server_started_after_five_seconds():
    await asyncio.sleep(5)
    print(app.config['method_runner'])
    print('Server successfully started!')

app.add_task(notify_server_started_after_five_seconds())

@app.route("/", methods=['GET', 'POST'])
async def root(request):
        data = request.json
        if 'method' in data:
            (module, method) = data['method'].split('.')
            # async job
            if method.startswith('_') and method.endswith('_submit'):
                subactions.append({'method': 'TODO'})
                data['method'] = '%s.%s' % (module, method[1:-7])
                job_id = str(uuid.uuid1())
                print("Starting subjob %s %s" % (job_id, data['method']))
                if os.fork()==0:
                    try:
                        mr.run(config, data, job_id, callback=callback_url, subjob=True)
                    except Exception as e:
                        print("Subjob %s failed" %(job_id))
                        print(e)
                    sys.exit()
                return json({'result': job_id})
            elif method.startswith('_check_job'):
                job_id = data['params'][0]
                resp = {'finished': False}
                if mr.is_finished(job_id):
                    resp = mr.get_output(job_id)
                    resp['finished'] = True
                    print("Subjob %s finished" % (job_id))
                    mr.cleanup(config, job_id)
                return json({'result': [resp]})
            elif method.startswith('get_provenance'):
                # TODO fill in prov
                print(subactions)
                prov = {'subactions': []}
                    # 'name': 'bogus',
                    # 'ver': 'bbogus',
                    # 'code_url': '<url>',  # TODO
                    # 'commit': '<hash>'  # TODO
                return json({'result':[None]})
            else:
                return json({'error': 'Unrecongnized post'})

        return json({})


#TODO: Implement get_prov, run sub job, and check job status

def _get_ip_address(ifname):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("gmail.com",80))
    ip = s.getsockname()[0]
    s.close()
    return ip


def start_callback_server(newconfig, method_runner):
    # Find a free port and Start up callback server with paths to tmp dirs, refdata, volume mounts/binds
    ip = _get_ip_address('en0')
    port = 8080
    url = 'http://%s:%s/' % (ip, port)
    config = newconfig
    mr = method_runner
    callback_url = url
    conf = {
        'method_runner': method_runner,
        'config': newconfig
    }
    app.config.update(conf)
    app.run(host=ip, port=port)
        
    return url


def stop_callback_server():
    if pid:
        os.kill(pid)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
