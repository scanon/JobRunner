import cherrypy
from cherrypy._cpserver import Server
import socket
import struct
import fcntl
import json
import uuid
import os
import sys
from runner.MethodRunner import MethodRunner

@cherrypy.expose
class Callback(object):
    def __init__(self, config, method_runner, callback=None, token=None, admintoken=None):
        self.config = config
        self.mr = method_runner
        self.callback = callback
        self.subactions = []

    def GET(self):
        return '{}}'

    def POST(self):
        cl = cherrypy.request.headers['Content-Length']
        rawbody = cherrypy.request.body.read(int(cl))
        data = json.loads(rawbody)
        if 'method' in data:
            (module, method) = data['method'].split('.')
            # async job
            if method.startswith('_') and method.endswith('_submit'):
                self.subactions.append({'method': 'TODO'})
                data['method'] = '%s.%s' % (module, method[1:-7])
                job_id = str(uuid.uuid1())
                print("Starting subjob %s %s" % (job_id, data['method']))
                if os.fork()==0:
                    try:
                        self.mr.run(self.config, data, job_id, callback=self.callback, subjob=True)
                    except Exception as e:
                        print("Subjob %s failed" %(job_id))
                        print(e)
                    sys.exit()
                return json.dumps({'result': job_id})
            elif method.startswith('_check_job'):
                job_id = data['params'][0]
                resp = {'finished': False}
                if self.mr.is_finished(job_id):
                    resp = self.mr.get_output(job_id)
                    resp['finished'] = True
                    print("Subjob %s finished" % (job_id))
                    self.mr.cleanup(self.config, job_id)
                return json.dumps({'result': [resp]})
            elif method.startswith('get_provenance'):
                # TODO fill in prov
                print(self.subactions)
                prov = {'subactions': []}
                    # 'name': 'bogus',
                    # 'ver': 'bbogus',
                    # 'code_url': '<url>',  # TODO
                    # 'commit': '<hash>'  # TODO
                return json.dumps({'result':[None]})
            else:
                return json.dumps({'error': 'Unrecongnized post'})

        return '{}'


#TODO: Implement get_prov, run sub job, and check job status

def _get_ip_address(ifname):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("gmail.com",80))
    ip = s.getsockname()[0]
    s.close()
    return ip


def start_callback_server(config, method_runner, token=None, admintoken=None):
    # Find a free port and Start up callback server with paths to tmp dirs, refdata, volume mounts/binds
    ip = _get_ip_address('en0')
    port = 8080
    cherrypy.config.update({
        'server.socket_host': ip,
        'server.socket_port': port,
        })
    cherrypy.config.update({'log.screen': False,
                        'log.access_file': '',
                        'log.error_file': ''})
    url = 'http://%s:%s/' % (ip, port)
    conf = {
        '/': {
            'request.dispatch': cherrypy.dispatch.MethodDispatcher(),
            'tools.sessions.on': True,
            'tools.response_headers.on': True,
            'tools.response_headers.headers': [('Content-Type', 'text/plain')],
        }
    }
    cherrypy.tree.mount(Callback(config, method_runner, callback=url, token=token, admintoken=admintoken), '/', conf)
    cherrypy.engine.start()
    return url


def stop_callback_server():
    cherrypy.engine.stop()
    cherrypy.engine.exit()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
